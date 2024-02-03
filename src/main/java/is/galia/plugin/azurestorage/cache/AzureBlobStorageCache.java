/*
 * Copyright © 2024 Baird Creek Software LLC
 *
 * Licensed under the PolyForm Noncommercial License, version 1.0.0;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://polyformproject.org/licenses/noncommercial/1.0.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package is.galia.plugin.azurestorage.cache;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import is.galia.async.VirtualThreadPool;
import is.galia.plugin.azurestorage.config.Key;
import is.galia.cache.AbstractCache;
import is.galia.stream.CompletableNullOutputStream;
import is.galia.stream.CompletableOutputStream;
import is.galia.cache.InfoCache;
import is.galia.cache.VariantCache;
import is.galia.config.Configuration;
import is.galia.image.Identifier;
import is.galia.image.Info;
import is.galia.image.StatResult;
import is.galia.operation.Encode;
import is.galia.operation.OperationList;
import is.galia.plugin.Plugin;
import is.galia.util.Stopwatch;
import is.galia.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static is.galia.config.Key.VARIANT_CACHE_TTL;

/**
 * <p>Cache using an Azure Blob Storage container.</p>
 *
 * <p>Blobs are named according to the following template:</p>
 *
 * <dl>
 *     <dt>Images</dt>
 *     <dd><code>{@link Key#AZUREBLOBSTORAGECACHE_BLOB_NAME_PREFIX}/image/{op
 *     list string representation}</code></dd>
 *     <dt>Info</dt>
 *     <dd><code>{@link Key#AZUREBLOBSTORAGECACHE_BLOB_NAME_PREFIX}/info/{identifier}.json</code></dd>
 * </dl>
 *
 * <p>The high-level partition between images and infos is meant to make it
 * more efficient to purge batches of one or the other.</p>
 *
 * <p>Azure Blob Storage does not enable Last-accessed times by default&mdash;
 * they must be manually enabled at the service level (see
 * <a href="https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview?tabs=azure-portal#move-data-based-on-last-accessed-time">
 *     Optimize costs by automatically managing the data lifecycle</a>).
 * If this is not done then last-modified times will be used instead.</p>
 *
 * @see <a href="https://github.com/azure/azure-storage-java">
 *     Microsoft Azure Storage SDK for Java</a>
 */
public final class AzureBlobStorageCache extends AbstractCache
        implements VariantCache, InfoCache, Plugin {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AzureBlobStorageCache.class);

    private static final String INFO_EXTENSION = ".json";

    private static BlobContainerClient cachedClient;

    /**
     * Names of blobs currently being written from any thread.
     */
    private final Set<String> uploadingNames = ConcurrentHashMap.newKeySet();

    private static synchronized BlobContainerClient getContainerClient() {
        if (cachedClient == null) {
            cachedClient = ContainerClientFactory.newContainerClient();
        }
        return cachedClient;
    }

    private static Instant getEarliestValidInstant() {
        final Configuration config = Configuration.forApplication();
        final long ttl = config.getLong(VARIANT_CACHE_TTL);
        return (ttl > 0) ?
                Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(ttl) :
                Instant.MIN;
    }

    /**
     * @return Blob name of the serialized {@link Info} associated with the
     *         given identifier.
     */
    String getBlobName(Identifier identifier) {
        return getBlobNamePrefix() + "info/" +
                StringUtils.md5(identifier.toString()) + INFO_EXTENSION;
    }

    /**
     * @return Blob name of the variant image associated with the given
     *         operation list.
     */
    String getBlobName(OperationList opList) {
        final String idStr  = StringUtils.md5(opList.getIdentifier().toString());
        final String opsStr = StringUtils.md5(opList.toString());
        String extension = "";
        Encode encode = (Encode) opList.getFirst(Encode.class);
        if (encode != null) {
            extension = "." + encode.getFormat().getPreferredExtension();
        }
        return String.format("%simage/%s/%s%s",
                getBlobNamePrefix(), idStr, opsStr, extension);
    }

    /**
     * @return Value of {@link Key#AZUREBLOBSTORAGECACHE_BLOB_NAME_PREFIX}
     *         with trailing slash.
     */
    String getBlobNamePrefix() {
        String prefix = Configuration.forApplication().
                getString(Key.AZUREBLOBSTORAGECACHE_BLOB_NAME_PREFIX.key());
        if (prefix == null || prefix.isBlank() || prefix.equals("/")) {
            return "";
        }
        return StringUtils.stripEnd(prefix, "/") + "/";
    }

    String getTempObjectName(OperationList opList) {
        return getBlobName(opList) + getTempObjectNameSuffix();
    }

    private String getTempObjectNameSuffix() {
        return "_" + Thread.currentThread().getName() + ".tmp";
    }

    /**
     * <p>N.B.: Last-accessed times are not available by default--they must be
     * manually enabled at the service level (see
     * <a href="https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview?tabs=azure-portal#move-data-based-on-last-accessed-time">
     *     Optimize costs by automatically managing the data lifecycle</a>).
     * If this is not done then the last-accessed time will be null and this
     * method will fall back to using the last-modified time.</p>
     */
    private boolean isValid(BlobClient blobClient) {
        OffsetDateTime time = blobClient.getProperties().getLastAccessedTime();
        if (time == null) {
            time = blobClient.getProperties().getLastModified();
        }
        return time.toInstant().isAfter(getEarliestValidInstant());
    }

    /**
     * @see #isValid(BlobClient)
     */
    private boolean isValid(BlobItem blobItem) {
        OffsetDateTime time = blobItem.getProperties().getLastAccessedTime();
        if (time == null) {
            time = blobItem.getProperties().getLastModified();
        }
        return time.toInstant().isAfter(getEarliestValidInstant());
    }

    private void purgeAsync(BlobClient blob) {
        VirtualThreadPool.getInstance().submit(() -> {
            LOGGER.debug("purgeAsync(): {}", blob);
            blob.deleteIfExists();
        });
    }

    //endregion
    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Arrays.stream(Key.values())
                .map(Key::toString)
                .filter(k -> k.contains(AzureBlobStorageCache.class.getSimpleName()))
                .collect(Collectors.toSet());
    }

    @Override
    public String getPluginName() {
        return getClass().getSimpleName();
    }

    @Override
    public void onApplicationStart() {}

    @Override
    public void onApplicationStop() {}

    @Override
    public void initializePlugin() {}

    //endregion
    //region Cache methods

    @Override
    public void evict(Identifier identifier) throws IOException {
        final BlobContainerClient containerClient = getContainerClient();
        int count = 0;
        try {
            // purge the info
            BlobClient blob = containerClient.getBlobClient(getBlobName(identifier));
            if (blob.deleteIfExists()) {
                count++;
            }

            // purge images
            final String prefix = getBlobNamePrefix() + "image/" +
                    StringUtils.md5(identifier.toString());
            final ListBlobsOptions options = new ListBlobsOptions();
            options.setPrefix(prefix);
            for (BlobItem item : containerClient.listBlobs(options, Duration.ofSeconds(30))) {
                LOGGER.debug("evict(Identifier): deleting {}",
                        item.getName());
                BlobClient blobClient = containerClient.getBlobClient(item.getName());
                if (blobClient.deleteIfExists()) {
                    count++;
                }
            }
            LOGGER.debug("evict(Identifier): deleted {} items", count);
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void evictInvalid() throws IOException {
        final BlobContainerClient containerClient = getContainerClient();
        int count = 0, deletedCount = 0;
        final ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(getBlobNamePrefix());
        try {
            for (BlobItem item : containerClient.listBlobs(options, Duration.ofSeconds(30))) {
                count++;
                if (!isValid(item)) {
                    BlobClient blobClient = containerClient.getBlobClient(item.getName());
                    if (blobClient.deleteIfExists()) {
                        deletedCount++;
                    }
                }
            }
            LOGGER.debug("evictInvalid(): deleted {} of {} items",
                    deletedCount, count);
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void purge() throws IOException {
        final BlobContainerClient containerClient = getContainerClient();
        int count = 0;
        final ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(getBlobNamePrefix());
        try {
            for (BlobItem item : containerClient.listBlobs(options, Duration.ofSeconds(30))) {
                BlobClient blobClient = containerClient.getBlobClient(item.getName());
                if (blobClient.deleteIfExists()) {
                    count++;
                }
            }
            LOGGER.debug("purge(): deleted {} items", count);
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    //endregion
    //region InfoCache methods

    @Override
    public void evictInfos() throws IOException {
        final BlobContainerClient containerClient = getContainerClient();
        int count = 0, deletedCount = 0;
        final ListBlobsOptions options = new ListBlobsOptions();
        options.setPrefix(getBlobNamePrefix());
        try {
            for (BlobItem item : containerClient.listBlobs(options, Duration.ofSeconds(30))) {
                count++;
                if (item.getName().endsWith(INFO_EXTENSION)) {
                    BlobClient blobClient = containerClient.getBlobClient(item.getName());
                    if (blobClient.deleteIfExists()) {
                        deletedCount++;
                    }
                }
            }
            LOGGER.debug("purgeInfos(): deleted {} of {} items",
                    deletedCount, count);
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Optional<Info> fetchInfo(Identifier identifier) throws IOException {
        final String containerName  = ContainerClientFactory.getConfiguredContainerName();
        final BlobContainerClient containerClient = getContainerClient();
        final Stopwatch watch       = new Stopwatch();
        final String objectName     = getBlobName(identifier);
        final BlobClient blobClient = containerClient.getBlobClient(objectName);
        try {
            if (isValid(blobClient)) {
                try (InputStream is = blobClient.openInputStream()) {
                    Info info = Info.fromJSON(is);
                    // Populate the serialization timestamp if it is not
                    // already, as suggested by the method contract.
                    if (info.getSerializationTimestamp() == null) {
                        info.setSerializationTimestamp(
                                blobClient.getProperties().getLastModified().toInstant());
                    }
                    LOGGER.debug("fetchInfo(): read {} from container {} in {}",
                            objectName, containerName, watch);
                    return Optional.of(info);
                }
            } else {
                LOGGER.debug("fetchInfo(): deleting invalid item " +
                                "asynchronously: {} in container {}",
                        objectName, containerName);
                purgeAsync(blobClient);
            }
            return Optional.empty();
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                return Optional.empty();
            }
            throw new IOException(e);
        }
    }

    @Override
    public void put(Identifier identifier, Info info) throws IOException {
        put(identifier, info.toJSON());
    }

    @Override
    public void put(Identifier identifier, String info) throws IOException {
        LOGGER.debug("put(): caching info for {}", identifier);
        final String blobName = getBlobName(identifier);
        if (!uploadingNames.contains(blobName)) {
            uploadingNames.add(blobName);
            BlobContainerClient containerClient = getContainerClient();
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            BlobHttpHeaders destBlobHeaders = new BlobHttpHeaders()
                    .setContentType("application/json")
                    .setContentEncoding("UTF-8");
            AzureBlobOutputStream os = new AzureBlobOutputStream(
                    blobClient, destBlobHeaders, uploadingNames);
            try (OutputStreamWriter writer = new OutputStreamWriter(os)) {
                writer.write(info);
            }
        }
    }

    //endregion
    //region VariantCache methods

    @Override
    public InputStream newVariantImageInputStream(
            OperationList opList,
            StatResult statResult) throws IOException {
        final BlobContainerClient containerClient = getContainerClient();
        final String containerName  = containerClient.getBlobContainerName();
        final String objectName     = getBlobName(opList);
        final BlobClient blobClient = containerClient.getBlobClient(objectName);

        LOGGER.debug("newVariantImageInputStream(): container: {}; name: {}",
                containerName, objectName);
        try {
            if (blobClient.exists()) {
                if (isValid(blobClient)) {
                    statResult.setLastModified(
                            blobClient.getProperties().getLastModified().toInstant());
                    return blobClient.openInputStream();
                } else {
                    LOGGER.debug("newVariantImageInputStream(): " +
                                    "deleting invalid item asynchronously: " +
                                    "{} in container {}",
                            objectName, containerName);
                    purgeAsync(blobClient);
                }
            }
            return null;
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public CompletableOutputStream
    newVariantImageOutputStream(OperationList opList) {
        final String objectName = getBlobName(opList);
        if (!uploadingNames.contains(objectName)) {
            uploadingNames.add(objectName);
            final String tempObjectName               = getTempObjectName(opList);
            final BlobContainerClient containerClient = getContainerClient();
            final BlobClient blobClient               =
                    containerClient.getBlobClient(tempObjectName);
            BlobHttpHeaders destBlobHeaders = new BlobHttpHeaders()
                    .setContentType(opList.getOutputFormat().
                            getPreferredMediaType().toString());
            return new AzureBlobOutputStream(
                    containerClient, blobClient, objectName, opList,
                    destBlobHeaders, uploadingNames, getAllObservers());
        }
        return new CompletableNullOutputStream();
    }

    @Override
    public void evict(OperationList opList) throws IOException {
        final BlobContainerClient containerClient = getContainerClient();
        final String objectName     = getBlobName(opList);
        final BlobClient blobClient = containerClient.getBlobClient(objectName);
        try {
            blobClient.deleteIfExists();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

}


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

package is.galia.plugin.azurestorage.source;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import is.galia.delegate.DelegateException;
import is.galia.plugin.azurestorage.config.Key;
import is.galia.plugin.Plugin;
import is.galia.stream.HTTPImageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.image.StatResult;
import is.galia.source.AbstractSource;
import is.galia.source.LookupStrategy;
import is.galia.source.Source;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>Maps an identifier to a
 * <a href="https://azure.microsoft.com/en-us/products/storage/blobs">Microsoft
 * Azure Storage</a> blob, for retrieving images from Azure Blob Storage.</p>
 *
 * <h1>Format Inference</h1>
 *
 * <p>See {@link BlobFormatIterator}.</p>
 *
 * <h1>Lookup Strategies</h1>
 *
 * <p>Two distinct lookup strategies are supported, defined by
 * {@link Key#AZUREBLOBSTORAGESOURCE_LOOKUP_STRATEGY}. BasicLookupStrategy maps
 * identifiers directly to blob names. DelegateLookupStrategy invokes a
 * delegate method to retrieve blob names dynamically.</p>
 *
 * <h1>Authentication</h1>
 *
 * <p>Authentication is via shared-key credentials. SAS URLs are not
 * supported&mdash;HTTPSource should be used for those instead.</p>
 *
 * <h1>Resource Access</h1>
 *
 * <p>While proceeding through the client request fulfillment flow, the
 * following server requests are sent:</p>
 *
 * <ol>
 *     <li>{@literal HEAD}</li>
 *     <li>
 *         <ol>
 *             <li>If the return value of {@link #getFormatIterator()} needs to
 *             check magic bytes:
 *                 <ol>
 *                     <li>Ranged {@literal GET}</li>
 *                 </ol>
 *             </li>
 *             <li>A series of ranged {@literal GET} requests (see {@link
 *             HTTPImageInputStream} for
 *             details)</li>
 *         </ol>
 *     </li>
 * </ol>
 *
 * @see <a href="https://learn.microsoft.com/en-us/java/api/overview/azure/storage-blob-readme">
 *     Azure Storage Blob Client Library for Java</a>
 */
public final class AzureBlobStorageSource extends AbstractSource
        implements Source, Plugin {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AzureBlobStorageSource.class);

    private BlobClient cachedBlobClient;
    private BlobInfo blobInfo;
    private BlobFormatIterator<Format> blobFormatIterator;

    BlobClient getBlobClient() throws IOException {
        if (cachedBlobClient == null) {
            BlobInfo blobInfo = getBlobInfo();
            BlobContainerClient containerClient =
                    ContainerClientFactory.newContainerClient(blobInfo);
            cachedBlobClient = containerClient.getBlobClient(blobInfo.blobName());
        }
        return cachedBlobClient;
    }

    BlobInfo getBlobInfo() throws IOException {
        if (blobInfo == null) {
            final LookupStrategy strategy =
                    LookupStrategy.from(Key.AZUREBLOBSTORAGESOURCE_LOOKUP_STRATEGY.key());
            //noinspection SwitchStatementWithTooFewBranches
            switch (strategy) {
                case DELEGATE_SCRIPT:
                    try {
                        blobInfo = getBlobInfoUsingDelegateStrategy();
                    } catch (DelegateException e) {
                        LOGGER.error(e.getMessage(), e);
                        throw new IOException(e);
                    }
                    break;
                default:
                    blobInfo = new BlobInfo(identifier.toString());
                    break;
            }
        }
        return blobInfo;
    }

    /**
     * @throws NoSuchFileException if the delegate script does not exist.
     * @throws DelegateException   if the delegate method throws an exception.
     */
    private BlobInfo getBlobInfoUsingDelegateStrategy()
            throws NoSuchFileException, DelegateException {
        @SuppressWarnings("unchecked")
        final Map<String,String> result = (Map<String,String>) getDelegate().invoke(
                DelegateMethod.AZUREBLOBSTORAGESOURCE_BLOB_INFO.toString());
        if (result == null || result.isEmpty()) {
            throw new NoSuchFileException(
                    DelegateMethod.AZUREBLOBSTORAGESOURCE_BLOB_INFO +
                            " returned nil for " + identifier);
        } else if (result.containsKey("container") && result.containsKey("name")) {
            return new BlobInfo(
                    result.get("endpoint"),
                    result.get("account_name"),
                    result.get("account_key"),
                    result.get("container"),
                    result.get("name"));
        } else {
            throw new IllegalArgumentException(
                    "Returned hash must include container and name");
        }
    }

    private void reset() {
        blobFormatIterator = null;
    }

    //endregion
    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Arrays.stream(Key.values())
                .map(Key::toString)
                .filter(k -> k.contains(AzureBlobStorageSource.class.getSimpleName()))
                .collect(Collectors.toSet());
    }

    @Override
    public String getPluginName() {
        return AzureBlobStorageSource.class.getSimpleName();
    }

    @Override
    public void onApplicationStart() {}

    @Override
    public void onApplicationStop() {}

    @Override
    public void initializePlugin() {}

    //endregion
    //region Source methods

    @Override
    public StatResult stat() throws IOException {
        BlobClient blobClient = getBlobClient();
        StatResult result     = new StatResult();
        try {
            BlobProperties properties = blobClient.getProperties();
            result.setLastModified(properties.getLastModified().toInstant());
            return result;
        } catch (BlobStorageException e) {
            switch (e.getStatusCode()) {
                case 403:
                    throw new AccessDeniedException("stat(): " + e.getMessage());
                case 404:
                    throw new NoSuchFileException("stat(): " + e.getMessage());
                default:
                    throw new IOException(e);
            }
        }
    }

    @Override
    public Iterator<Format> getFormatIterator() {
        if (blobFormatIterator == null) {
            try {
                blobFormatIterator = new BlobFormatIterator<>(
                        getBlobClient(), getBlobInfo().blobName(),
                        getIdentifier());
            } catch (IOException e) {
                LOGGER.error("getFormatIterator(): {}", e.getMessage());
            }
        }
        return blobFormatIterator;
    }

    @Override
    public ImageInputStream newInputStream() throws IOException {
        return new AzureBlobStreamFactory(getBlobClient()).newSeekableStream();
    }

    @Override
    public void setIdentifier(Identifier identifier) {
        super.setIdentifier(identifier);
        reset();
    }

}

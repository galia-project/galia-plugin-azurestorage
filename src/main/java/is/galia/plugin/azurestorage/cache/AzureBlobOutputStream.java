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
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobBeginCopyOptions;
import com.azure.storage.blob.specialized.BlobOutputStream;
import is.galia.cache.CacheObserver;
import is.galia.stream.CompletableOutputStream;
import is.galia.operation.OperationList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

final class AzureBlobOutputStream extends CompletableOutputStream {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AzureBlobOutputStream.class);

    private final BlobClient blobClient;
    private final BlobHttpHeaders blobHeaders;
    private final String blobName;
    private final Set<String> uploadingNames;
    private BlobContainerClient containerClient;
    private OperationList opList;
    private Set<CacheObserver> observers = Collections.emptySet();
    private BlobOutputStream blobOutputStream;

    /**
     * Constructor for an instance that writes directly into the given blob.
     *
     * @param blobClient     Blob to write to.
     * @param blobHeaders    Destination headers.
     * @param uploadingNames Names of all blobs that are currently being
     *                       uploaded in any thread, including the one used by
     *                       {@code blobClient}, which {@link #close()} will
     *                       remove.
     */
    AzureBlobOutputStream(BlobClient blobClient,
                          BlobHttpHeaders blobHeaders,
                          Set<String> uploadingNames) {
        this.blobClient     = blobClient;
        this.blobHeaders    = blobHeaders;
        this.blobName       = blobClient.getBlobName();
        this.uploadingNames = uploadingNames;
    }

    /**
     * Constructor for an instance that writes into the given temporary blob.
     * Upon closure, if the stream is {@link #isComplete() completely written},
     * the temporary blob is copied into place and deleted. Otherwise, the
     * temporary blob is deleted.
     *
     * @param containerClient   Container housing the blobs.
     * @param tempBlob          Blob to write to.
     * @param permanentBlobName Name of the permanent blob, which {@code
     *                          tempBlob} will be copied to.
     * @param opList            Instance describing the image being written.
     * @param blobHeaders       Destination headers.
     * @param uploadingNames    Names of all blobs that are currently being
     *                          uploaded in any thread, including {@code
     *                          permanentBlobName}, which {@link #close()} will
     *                          remove.
     * @param observers         Observers of the cache utilizing this instance.
     */
    AzureBlobOutputStream(BlobContainerClient containerClient,
                          BlobClient tempBlob,
                          String permanentBlobName,
                          OperationList opList,
                          BlobHttpHeaders blobHeaders,
                          Set<String> uploadingNames,
                          Set<CacheObserver> observers) {
        this.containerClient = containerClient;
        this.blobClient      = tempBlob;
        this.blobHeaders     = blobHeaders;
        this.blobName        = permanentBlobName;
        this.opList          = opList;
        this.uploadingNames  = uploadingNames;
        this.observers       = observers;
    }

    @Override
    public void close() throws IOException {
        try {
            if (blobOutputStream != null) {
                blobOutputStream.close();
            }
            if (containerClient != null) { // if constructor 2
                if (isComplete()) {
                    // Copy the temporary blob into place.
                    BlobClient destBlob =
                            containerClient.getBlobClient(blobName);
                    BlobBeginCopyOptions options =
                            new BlobBeginCopyOptions(blobClient.getBlobUrl());
                    destBlob.beginCopy(options);
                    blobClient.setHttpHeaders(blobHeaders);
                }
                blobClient.deleteIfExists();
                observers.forEach(o -> o.onImageWritten(opList));
            }
        } catch (BlobStorageException e) {
            if ("BlobAlreadyExists".equals(e.getErrorCode().toString())) {
                // This is likely in the case of multiple concurrent writes to
                // the same blob, and is not an issue.
                LOGGER.debug("close(): " + e.getMessage());
            } else {
                throw new IOException(e);
            }
        } catch (IOException e) {
            if (e.getMessage().contains("BlobAlreadyExists")) {
                LOGGER.debug("close(): " + e.getMessage());
            } else {
                throw e;
            }
        } finally {
            try {
                super.close();
            } finally {
                uploadingNames.remove(blobName);
            }
        }
    }

    private boolean openOutputStream() throws IOException {
        if (blobOutputStream != null) {
            return true;
        }
        try {
            blobOutputStream =
                    blobClient.getBlockBlobClient().getBlobOutputStream(true);
            return true;
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("already exists")) {
                // This is likely in the case of multiple concurrent writes to
                // the same blob, and is not an issue.
                LOGGER.debug("openOutputStream(): " + e.getMessage());
            } else {
                throw new IOException(e);
            }
            return false;
        }
    }

    @Override
    public void flush() throws IOException {
        if (openOutputStream()) {
            blobOutputStream.flush();
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (openOutputStream()) {
            blobOutputStream.write(b);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (openOutputStream()) {
            blobOutputStream.write(b);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (openOutputStream()) {
            blobOutputStream.write(b, off, len);
        }
    }

}

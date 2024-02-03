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
import com.azure.storage.blob.models.BlobStorageException;
import is.galia.plugin.azurestorage.config.Key;
import is.galia.stream.ClosingFileCacheImageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import is.galia.config.Configuration;
import is.galia.stream.HTTPImageInputStream;
import is.galia.util.IOUtils;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;

/**
 * Source of streams for {@link AzureBlobStorageSource}, returned from {@link
 * AzureBlobStorageSource#newInputStream()}.
 */
class AzureBlobStreamFactory {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AzureBlobStreamFactory.class);

    private static final int DEFAULT_CHUNK_SIZE       = 1024 * 512;
    private static final int DEFAULT_CHUNK_CACHE_SIZE = 1024 * 1024 * 10;

    private final BlobClient blobClient;

    AzureBlobStreamFactory(BlobClient blobClient) {
        this.blobClient = blobClient;
    }

    ImageInputStream newSeekableStream() throws IOException {
        if (isChunkingEnabled()) {
            final int chunkSize = getChunkSize();
            LOGGER.debug("newSeekableStream(): using {}-byte chunks",
                    chunkSize);

            final AzureBlobHTTPImageInputStreamClient client =
                    new AzureBlobHTTPImageInputStreamClient(blobClient);
            HTTPImageInputStream stream = null;

            try {
                // Populate the blob's properties, if they haven't been already.
                blobClient.exists();

                stream = new HTTPImageInputStream(
                        client, blobClient.getProperties().getBlobSize());

                stream.setWindowSize(chunkSize);
                return stream;
            } catch (BlobStorageException e) {
                switch (e.getStatusCode()) {
                    case 403:
                        throw new AccessDeniedException("newSeekableStream(): " +
                                e.getMessage());
                    case 404:
                        throw new NoSuchFileException("newSeekableStream(): " +
                                e.getMessage());
                    default:
                        throw new IOException(e);
                }
            } catch (Throwable t) {
                IOUtils.closeQuietly(stream);
                throw t;
            }
        } else {
            LOGGER.debug("newSeekableStream(): chunking is disabled");
            return new ClosingFileCacheImageInputStream(
                    blobClient.openInputStream());
        }
    }

    private boolean isChunkingEnabled() {
        return Configuration.forApplication().getBoolean(
                Key.AZUREBLOBSTORAGESOURCE_CHUNKING_ENABLED.key(), true);
    }

    private int getChunkSize() {
        return (int) Configuration.forApplication().getLongBytes(
                Key.AZUREBLOBSTORAGESOURCE_CHUNK_SIZE.key(), DEFAULT_CHUNK_SIZE);
    }

}

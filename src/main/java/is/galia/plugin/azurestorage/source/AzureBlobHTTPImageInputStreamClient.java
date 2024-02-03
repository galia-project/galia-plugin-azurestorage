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

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DownloadRetryOptions;
import is.galia.http.MutableResponse;
import is.galia.http.Range;
import is.galia.http.Reference;
import is.galia.http.Response;
import is.galia.http.Status;
import is.galia.stream.HTTPImageInputStreamClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;

/**
 * Implementation backed by an Azure Blob Storage client.
 */
public class AzureBlobHTTPImageInputStreamClient
        implements HTTPImageInputStreamClient {

    private final BlobClient blobClient;

    AzureBlobHTTPImageInputStreamClient(BlobClient blobClient) {
        this.blobClient = blobClient;
    }

    @Override
    public Reference getReference() {
        return new Reference(blobClient.getBlobUrl());
    }

    @Override
    public Response sendHEADRequest() throws IOException {
        try {
            blobClient.exists(); // will send the HEAD request if it hasn't been done already
            final MutableResponse response = new MutableResponse();
            response.setStatus(Status.OK);
            response.getHeaders().set("Content-Length",
                    Long.toString(blobClient.getProperties().getBlobSize()));
            response.getHeaders().set("Accept-Ranges", "bytes");
            return response;
        } catch (BlobStorageException e) {
            switch (e.getStatusCode()) {
                case 403:
                    throw new AccessDeniedException("sendHEADRequest(): " + e.getMessage());
                case 404:
                    throw new NoSuchFileException("sendHEADRequest(): " + e.getMessage());
                default:
                    throw new IOException(e);
            }
        }
    }

    @Override
    public Response sendGETRequest(Range range) throws IOException {
        final long length                 = range.end() - range.start() + 1;
        final BlobRange blobRange         = new BlobRange(range.start(), length);
        DownloadRetryOptions retryOptions = new DownloadRetryOptions();
        BlobRequestConditions conditions  = new BlobRequestConditions();
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            blobClient.downloadStreamWithResponse(outputStream, blobRange,
                    retryOptions, conditions, false,
                    Duration.ofSeconds(30),
                    Context.NONE);
            final MutableResponse response = new MutableResponse();
            response.setStatus(Status.PARTIAL_CONTENT);
            response.setBody(outputStream.toByteArray());
            return response;
        } catch (BlobStorageException e) {
            switch (e.getStatusCode()) {
                case 403:
                    throw new AccessDeniedException("sendGETRequest(): " + e.getMessage());
                case 404:
                    throw new NoSuchFileException("sendGETRequest(): " + e.getMessage());
                default:
                    throw new IOException(e);
            }
        }
    }

}

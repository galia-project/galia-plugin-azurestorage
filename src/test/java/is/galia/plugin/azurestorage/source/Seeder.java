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
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import is.galia.plugin.azurestorage.test.TestUtils;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

class Seeder {

    static final String BLOB_NAME_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION      = "ghost1.png";
    static final String BLOB_NAME_WITH_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION    = "ghost1.unknown";
    static final String BLOB_NAME_WITH_CONTENT_TYPE_BUT_NO_EXTENSION              = "ghost1";
    static final String BLOB_NAME_WITH_NO_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION   = "ghost2.png";
    static final String BLOB_NAME_WITH_NO_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION = "ghost2.unknown";
    static final String BLOB_NAME_WITH_NO_CONTENT_TYPE_AND_INCORRECT_EXTENSION    = "ghost2.jpg";
    static final String BLOB_NAME_WITH_NO_CONTENT_TYPE_OR_EXTENSION               = "ghost2";
    static final String NON_IMAGE_BLOB_NAME = "NotAnImage";

    static void emptyContainer() {
        final BlobContainerClient containerClient = ContainerClientFactory.newContainerClient();
        if (containerClient.exists()) {
            for (BlobItem item : containerClient.listBlobs()) {
                BlobClient client = containerClient.getBlobClient(item.getName());
                client.deleteIfExists();
            }
        }
    }

    static void uploadFixtures() throws Exception {
        final BlobContainerClient containerClient =
                ContainerClientFactory.newContainerClient();
        containerClient.createIfNotExists();

        Path fixture = TestUtils.getFixture("ghost.png");

        for (final String name : new String[] {
                BLOB_NAME_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION,
                BLOB_NAME_WITH_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION,
                BLOB_NAME_WITH_CONTENT_TYPE_BUT_NO_EXTENSION,
                BLOB_NAME_WITH_NO_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION,
                BLOB_NAME_WITH_NO_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION,
                BLOB_NAME_WITH_NO_CONTENT_TYPE_AND_INCORRECT_EXTENSION,
                BLOB_NAME_WITH_NO_CONTENT_TYPE_OR_EXTENSION}) {
            final BlobClient blobClient = containerClient.getBlobClient(name);
            // Upload the blob.
            try (OutputStream os = blobClient.getBlockBlobClient().getBlobOutputStream()) {
                Files.copy(fixture, os);
            }
            // Add a Content-Type to the blobs that are supposed to have one.
            if (BLOB_NAME_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION.equals(name) ||
                    BLOB_NAME_WITH_CONTENT_TYPE_AND_UNRECOGNIZED_EXTENSION.equals(name) ||
                    BLOB_NAME_WITH_CONTENT_TYPE_BUT_NO_EXTENSION.equals(name)) {
                BlobProperties properties = blobClient.getProperties();
                BlobHttpHeaders destBlobHeaders = new BlobHttpHeaders()
                        .setContentType("image/png")
                        .setCacheControl(properties.getCacheControl())
                        .setContentDisposition(properties.getContentDisposition())
                        .setContentMd5(properties.getContentMd5());
                blobClient.setHttpHeaders(destBlobHeaders);
            }
        }

        // Add a non-image
        fixture = TestUtils.getFixture("non_image.txt");
        final BlobClient blobClient = containerClient.getBlobClient(NON_IMAGE_BLOB_NAME);
        try (OutputStream os = blobClient.getBlockBlobClient().getBlobOutputStream()) {
            Files.copy(fixture, os);
        }
    }

    private Seeder() {}

}

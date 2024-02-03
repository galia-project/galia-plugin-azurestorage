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
import is.galia.http.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import is.galia.plugin.azurestorage.BaseTest;
import is.galia.http.Range;
import is.galia.http.Response;

import static org.junit.jupiter.api.Assertions.*;

class AzureBlobHTTPImageInputStreamClientTest extends BaseTest {

    private AzureBlobHTTPImageInputStreamClient instance;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        Seeder.emptyContainer();
        Seeder.uploadFixtures();
        BlobClient blobClient = ContainerClientFactory
                .newContainerClient()
                .getBlobClient("ghost1.png");
        instance = new AzureBlobHTTPImageInputStreamClient(blobClient);
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.setUp();
        Seeder.emptyContainer();
    }

    @Test
    void getReference() {
        assertNotNull(instance.getReference());
    }

    @Test
    void sendHEADRequest() throws Exception {
        try (Response actual = instance.sendHEADRequest()) {
            assertEquals(Status.OK, actual.getStatus());
            assertEquals("bytes", actual.getHeaders().getFirstValue("Accept-Ranges"));
            assertEquals("21103", actual.getHeaders().getFirstValue("Content-Length"));
        }
    }

    @Test
    void sendGETRequest() throws Exception {
        try (Response actual = instance.sendGETRequest(new Range(10, 50, 5439))) {
            assertEquals(Status.PARTIAL_CONTENT, actual.getStatus());
            assertEquals(41, actual.getBody().length);
        }
    }

}

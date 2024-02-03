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
import is.galia.config.Configuration;
import is.galia.plugin.azurestorage.config.Key;
import is.galia.plugin.azurestorage.BaseTest;
import is.galia.stream.ClosingFileCacheImageInputStream;
import is.galia.stream.HTTPImageInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.imageio.stream.ImageInputStream;

import static org.junit.jupiter.api.Assertions.*;

class AzureBlobStreamFactoryTest extends BaseTest {

    private AzureBlobStreamFactory instance;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        Seeder.emptyContainer();
        Seeder.uploadFixtures();
        BlobClient blobClient = ContainerClientFactory.newContainerClient()
                .getBlobClient("ghost1.png");
        instance = new AzureBlobStreamFactory(blobClient);
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.setUp();
        Seeder.emptyContainer();
    }

    @Test
    void newSeekableStreamTypeWithChunkingEnabled() throws Exception {
        final Configuration config = Configuration.forApplication();
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_CHUNKING_ENABLED.key(), true);
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_CHUNK_SIZE.key(), "777K");

        try (ImageInputStream is = instance.newSeekableStream()) {
            assertInstanceOf(HTTPImageInputStream.class, is);
            assertEquals(777 * 1024, ((HTTPImageInputStream) is).getWindowSize());
        }
    }

    @Test
    void newSeekableStreamLengthWithChunkingEnabled() throws Exception {
        final Configuration config = Configuration.forApplication();
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_CHUNKING_ENABLED.key(), true);
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_CHUNK_SIZE.key(), "1K");

        int length = 0;
        try (ImageInputStream is = instance.newSeekableStream()) {
            while (is.read() != -1) {
                length++;
            }
        }
        assertEquals(21103, length);
    }

    @Test
    void newSeekableStreamTypeWithChunkingDisabled() throws Exception {
        final Configuration config = Configuration.forApplication();
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_CHUNKING_ENABLED.key(), false);

        try (ImageInputStream is = instance.newSeekableStream()) {
            assertInstanceOf(ClosingFileCacheImageInputStream.class, is);
        }
    }

    @Test
    void newSeekableStreamLengthWithChunkingDisabled() throws Exception {
        final Configuration config = Configuration.forApplication();
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_CHUNKING_ENABLED.key(), false);

        int length = 0;
        try (ImageInputStream is = instance.newSeekableStream()) {
            while (is.read() != -1) {
                length++;
            }
        }
        assertEquals(21103, length);
    }

    @Test
    void newSeekableStreamWithChunkingEnabled() throws Exception {
        final Configuration config = Configuration.forApplication();
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_CHUNKING_ENABLED.key(), true);
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_CHUNK_SIZE.key(), "777K");

        try (ImageInputStream is = instance.newSeekableStream()) {
            HTTPImageInputStream htis = (HTTPImageInputStream) is;
            assertEquals(777 * 1024, htis.getWindowSize());
        }
    }

}

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

import is.galia.plugin.azurestorage.BaseTest;
import is.galia.image.Format;
import is.galia.image.Identifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class BlobFormatIteratorTest extends BaseTest {

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        Seeder.emptyContainer();
        Seeder.uploadFixtures();
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.setUp();
        Seeder.emptyContainer();
    }

    @Test
    void hasNext() throws Exception {
        AzureBlobStorageSource source = new AzureBlobStorageSource();
        source.setIdentifier(new Identifier(
                Seeder.BLOB_NAME_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION));
        BlobFormatIterator<Format> instance = new BlobFormatIterator<>(
                source.getBlobClient(), source.getBlobInfo().blobName(),
                source.getIdentifier());

        assertTrue(instance.hasNext());
        instance.next(); // blob name
        assertTrue(instance.hasNext());
        instance.next(); // identifier extension
        assertTrue(instance.hasNext());
        instance.next(); // Content-Type is null
        assertTrue(instance.hasNext());
        instance.next(); // magic bytes
        assertFalse(instance.hasNext());
    }

    @Test
    void getFormatIteratorNext() throws Exception {
        AzureBlobStorageSource source = new AzureBlobStorageSource();
        source.setIdentifier(new Identifier(
                Seeder.BLOB_NAME_WITH_NO_CONTENT_TYPE_AND_INCORRECT_EXTENSION));
        BlobFormatIterator<Format> instance = new BlobFormatIterator<>(
                source.getBlobClient(), source.getBlobInfo().blobName(),
                source.getIdentifier());

        assertEquals(Format.get("jpg"), instance.next()); // object key
        assertEquals(Format.get("jpg"), instance.next()); // identifier extension
        assertEquals(Format.UNKNOWN, instance.next());    // Content-Type is null
        assertEquals(Format.get("png"), instance.next()); // magic bytes
        assertThrows(NoSuchElementException.class, instance::next);
    }

}

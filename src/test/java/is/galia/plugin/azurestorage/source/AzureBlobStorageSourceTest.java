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

import is.galia.plugin.azurestorage.config.Key;
import is.galia.plugin.azurestorage.BaseTest;
import is.galia.plugin.azurestorage.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import is.galia.config.Configuration;
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.delegate.Delegate;
import is.galia.image.StatResult;
import is.galia.source.Source;

import javax.imageio.stream.ImageInputStream;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests {@link AzureBlobStorageSource} against Azure Storage. (Requires an Azure
 * account.)
 */
class AzureBlobStorageSourceTest extends BaseTest {

    private AzureBlobStorageSource instance;

    @BeforeAll
    public static void beforeClass() {
        new AzureBlobStorageSource().onApplicationStart();
    }

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        Seeder.emptyContainer();
        Seeder.uploadFixtures();
        useBasicLookupStrategy();
        instance = newInstance();
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.setUp();
        Seeder.emptyContainer();
    }

    AzureBlobStorageSource newInstance() {
        AzureBlobStorageSource instance = new AzureBlobStorageSource();
        instance.initializePlugin();
        instance.setIdentifier(new Identifier(Seeder.BLOB_NAME_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION));
        return instance;
    }

    void useBasicLookupStrategy() {
        Configuration config = Configuration.forApplication();
        config.setProperty(Key.AZUREBLOBSTORAGESOURCE_LOOKUP_STRATEGY.key(),
                "BasicLookupStrategy");
    }

    void useDelegateLookupStrategy() {
        try {
            Configuration config = Configuration.forApplication();
            config.setProperty(Key.AZUREBLOBSTORAGESOURCE_LOOKUP_STRATEGY.key(),
                    "DelegateLookupStrategy");
            Identifier identifier = new Identifier(Seeder.BLOB_NAME_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION);
            Delegate delegate = TestUtils.newDelegate();
            delegate.getRequestContext().setIdentifier(identifier);
            instance.setDelegate(delegate);
        } catch (Exception e) {
            fail();
        }
    }

    //region Plugin methods

    @Test
    void getPluginConfigKeys() {
        Set<String> keys = instance.getPluginConfigKeys();
        assertFalse(keys.isEmpty());
    }

    @Test
    void getPluginName() {
        assertEquals(AzureBlobStorageSource.class.getSimpleName(),
                instance.getPluginName());
    }

    //endregion
    //region Source methods

    /* getFormatIterator() */

    @Test
    void getFormatIteratorHasNext() {
        AzureBlobStorageSource source = newInstance();
        source.setIdentifier(new Identifier(
                Seeder.BLOB_NAME_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION));

        Iterator<Format> it = source.getFormatIterator();
        assertTrue(it.hasNext());
        it.next(); // blob name
        assertTrue(it.hasNext());
        it.next(); // identifier extension
        assertTrue(it.hasNext());
        it.next(); // Content-Type is null
        assertTrue(it.hasNext());
        it.next(); // magic bytes
        assertFalse(it.hasNext());
    }

    @Test
    void getFormatIteratorNext() {
        AzureBlobStorageSource source = newInstance();
        source.setIdentifier(new Identifier(
                Seeder.BLOB_NAME_WITH_NO_CONTENT_TYPE_AND_INCORRECT_EXTENSION));

        Iterator<Format> it = source.getFormatIterator();
        assertEquals(Format.get("jpg"), it.next()); // object key
        assertEquals(Format.get("jpg"), it.next()); // identifier extension
        assertEquals(Format.UNKNOWN, it.next());    // Content-Type is null
        assertEquals(Format.get("png"), it.next()); // magic bytes
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void getFormatIteratorConsecutiveInvocationsReturnSameInstance() {
        AzureBlobStorageSource instance = newInstance();
        var it = instance.getFormatIterator();
        assertSame(it, instance.getFormatIterator());
    }

    /* newInputStream() */

    @Test
    void newInputStreamUsingBasicLookupStrategy() throws Exception {
        try (ImageInputStream is = instance.newInputStream()) {
            assertNotNull(is);
        }
    }

    @Test
    void newInputStreamUsingDelegateLookupStrategy() throws Exception {
        useDelegateLookupStrategy();
        try (ImageInputStream is = instance.newInputStream()) {
            assertNotNull(is);
        }
    }

    /* getBlobInfo() */

    @Test
    void getBlobInfo() throws Exception {
        assertNotNull(instance.getBlobInfo());
    }

    @Test
    void getBlobInfoUsingBasicLookupStrategy() throws Exception {
        BlobInfo blobInfo = instance.getBlobInfo();
        assertEquals(Seeder.BLOB_NAME_WITH_CONTENT_TYPE_AND_RECOGNIZED_EXTENSION,
                blobInfo.blobName());
    }

    @Test
    void getBlobInfoUsingDelegateLookupStrategy() throws Exception {
        useDelegateLookupStrategy();
        BlobInfo blobInfo = instance.getBlobInfo();
        assertEquals(Seeder.BLOB_NAME_WITH_CONTENT_TYPE_BUT_NO_EXTENSION,
                blobInfo.blobName());
    }

    /* stat() */

    @Test
    void testStatUsingBasicLookupStrategyWithPresentReadableBlob()
            throws Exception {
        newInstance().stat();
    }

    @Test
    void statUsingBasicLookupStrategyWithPresentUnreadableBlob() {
        // TODO: write this
    }

    @Test
    void testStatUsingBasicLookupStrategyWithMissingBlob() {
        Source instance = newInstance();
        instance.setIdentifier(new Identifier("bogus"));
        assertThrows(NoSuchFileException.class, instance::stat);
    }

    @Test
    void statUsingDelegateLookupStrategyWithPresentReadableBlob()
            throws Exception {
        useDelegateLookupStrategy();
        instance.stat();
    }

    @Test
    void statUsingDelegateLookupStrategyWithPresentUnreadableBlob() {
        useDelegateLookupStrategy();
        // TODO: write this
    }

    @Test
    void statUsingDelegateLookupStrategyWithMissingBlob() {
        useDelegateLookupStrategy();

        Identifier identifier = new Identifier("bogus");
        Delegate delegate = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);

        assertThrows(NoSuchFileException.class, instance::stat);
    }

    @Test
    void testStatReturnsCorrectInstance() throws Exception {
        StatResult result = newInstance().stat();
        assertNotNull(result.getLastModified());
    }

    /**
     * Tests that {@link Source#stat()} can be invoked multiple times without
     * throwing an exception.
     */
    @Test
    void testStatInvokedMultipleTimes() throws Exception {
        Source instance = newInstance();
        instance.stat();
        instance.stat();
        instance.stat();
    }

}

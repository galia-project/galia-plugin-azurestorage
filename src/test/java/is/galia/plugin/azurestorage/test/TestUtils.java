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

package is.galia.plugin.azurestorage.test;

import is.galia.cache.VariantCache;
import is.galia.delegate.Delegate;
import is.galia.delegate.DelegateException;
import is.galia.operation.OperationList;
import is.galia.plugin.azurestorage.source.TestDelegate;
import is.galia.resource.RequestContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public final class TestUtils {

    public static void assertExists(VariantCache cache,
                                    OperationList opList) {
        try (InputStream is = cache.newVariantImageInputStream(opList)) {
            assertNotNull(is);
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public static void assertNotExists(VariantCache cache,
                                       OperationList opList) {
        try (InputStream is = cache.newVariantImageInputStream(opList)) {
            assertNull(is);
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public static File getCurrentWorkingDirectory() {
        try {
            return new File(".").getCanonicalFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path getFixture(String filename) {
        return getFixturePath().resolve(filename);
    }

    /**
     * @return Path of the fixtures directory.
     */
    public static Path getFixturePath() {
        return Paths.get(getCurrentWorkingDirectory().getAbsolutePath(),
                "src", "test", "resources");
    }

    public static Delegate newDelegate() {
        try {
            Delegate delegate = new TestDelegate();
            delegate.setRequestContext(new RequestContext());
            return delegate;
        } catch (DelegateException e) {
            throw new RuntimeException(e);
        }
    }

    private TestUtils() {}

}

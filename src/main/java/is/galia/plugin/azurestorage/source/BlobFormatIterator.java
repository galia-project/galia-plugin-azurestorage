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
import com.azure.storage.blob.models.DownloadRetryOptions;
import is.galia.codec.FormatDetector;
import is.galia.stream.ByteArrayImageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.image.MediaType;
import is.galia.source.FormatChecker;
import is.galia.source.IdentifierFormatChecker;
import is.galia.source.NameFormatChecker;

import javax.imageio.stream.ImageInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * <ol>
 *     <li>If the blob name has a recognized filename extension, the format
 *     is inferred from that.</li>
 *     <li>Otherwise, if the source image's URI identifier has a recognized
 *     filename extension, the format is inferred from that.</li>
 *     <li>Otherwise, a {@literal HEAD} request will be sent. If a {@literal
 *     Content-Type} header is present in the response, and is specific
 *     enough (i.e. not {@literal application/octet-stream}), a format is
 *     inferred from that.</li>
 *     <li>Otherwise, a {@literal GET} request will be sent with a {@literal
 *     Range} header specifying a small range of data from the beginning of
 *     the resource, and a format is inferred from the magic bytes in the
 *     response body.</li>
 * </ol>
 *
 * @param <T> {@link Format}.
 */
final class BlobFormatIterator<T> implements Iterator<T> {

    /**
     * Infers a {@link Format} based on the media type in a {@literal
     * Content-Type} header.
     */
    private class ContentTypeHeaderChecker implements FormatChecker {
        @Override
        public Format check() {
            final String contentType = blobClient.getProperties().getContentType();
            if (contentType != null && !contentType.isEmpty()) {
                return MediaType.fromString(contentType).toFormat();
            }
            return Format.UNKNOWN;
        }
    }

    /**
     * Infers a {@link Format} based on image magic bytes.
     */
    private class ByteChecker implements FormatChecker {
        @Override
        public Format check() throws IOException {
            final BlobRange blobRange         = new BlobRange(0, (long) FormatDetector.RECOMMENDED_READ_LENGTH);
            DownloadRetryOptions retryOptions = new DownloadRetryOptions();
            BlobRequestConditions conditions  = new BlobRequestConditions();
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                blobClient.downloadStreamWithResponse(
                        outputStream, blobRange,
                        retryOptions, conditions, false,
                        Duration.ofSeconds(30),
                        Context.NONE);
                try (ImageInputStream iis = new ByteArrayImageInputStream(outputStream.toByteArray())) {
                    return FormatDetector.detect(iis);
                }
            }
        }
    }

    private static final Logger LOGGER =
            LoggerFactory.getLogger(BlobFormatIterator.class);

    private final BlobClient blobClient;
    private final String blobName;
    private final Identifier identifier;
    private FormatChecker formatChecker;

    BlobFormatIterator(BlobClient blobClient,
                       String blobName,
                       Identifier identifier) {
        this.blobClient = blobClient;
        this.blobName   = blobName;
        this.identifier = identifier;
    }

    @Override
    public boolean hasNext() {
        return (formatChecker == null ||
                formatChecker instanceof NameFormatChecker ||
                formatChecker instanceof IdentifierFormatChecker ||
                formatChecker instanceof BlobFormatIterator.ContentTypeHeaderChecker);
    }

    @Override
    public T next() {
        if (formatChecker == null) {
            formatChecker = new NameFormatChecker(blobName);
        } else if (formatChecker instanceof NameFormatChecker) {
            formatChecker = new IdentifierFormatChecker(identifier);
        } else if (formatChecker instanceof IdentifierFormatChecker) {
            formatChecker = new ContentTypeHeaderChecker();
        } else if (formatChecker instanceof BlobFormatIterator.ContentTypeHeaderChecker) {
            formatChecker = new ByteChecker();
        } else {
            throw new NoSuchElementException();
        }
        try {
            //noinspection unchecked
            return (T) formatChecker.check();
        } catch (IOException e) {
            LOGGER.warn("Error checking format: {}", e.getMessage());
            //noinspection unchecked
            return (T) Format.UNKNOWN;
        }
    }

}
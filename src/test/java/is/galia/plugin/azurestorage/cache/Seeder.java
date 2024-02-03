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
import com.azure.storage.blob.models.BlobItem;

class Seeder {

    static void emptyContainer() {
        final BlobContainerClient containerClient = ContainerClientFactory.newContainerClient();
        if (containerClient.exists()) {
            for (BlobItem item : containerClient.listBlobs()) {
                BlobClient client = containerClient.getBlobClient(item.getName());
                client.deleteIfExists();
            }
        }
    }

    private Seeder() {}

}

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

/**
 * Contains information needed to access an Azure blob.
 */
record BlobInfo(String endpoint, String accountName, String accountKey,
                String containerName, String blobName) {

    BlobInfo(String blobName) {
        this(null, null, null, null, blobName);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[endpoint: ").append(endpoint).append("] ");
        String tmp = accountName != null ? "******" : "null";
        b.append("[accountName: ").append(tmp).append("] ");
        tmp = accountKey != null ? "******" : "null";
        b.append("[accountKey: ").append(tmp).append("] ");
        b.append("[container: ").append(containerName).append("] ");
        b.append("[name: ").append(blobName).append("]");
        return b.toString();
    }

}

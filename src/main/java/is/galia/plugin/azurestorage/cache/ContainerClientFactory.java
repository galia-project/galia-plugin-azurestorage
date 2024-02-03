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

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import is.galia.async.VirtualThreadPool;
import is.galia.config.Configuration;
import is.galia.plugin.azurestorage.config.Key;
import is.galia.util.StringUtils;

/**
 * <p>Builds Azure Blob Storage container clients.</p>
 *
 * <p>The endpoint is obtained from the following sources:</p>
 *
 * <ol>
 *     <li>The application configuration</li>
 *     <li>The default Azure Blob Storage endpoint</li>
 * </ol>
 *
 * <p>The account name and key are obtained from the application
 * configuration. If the account key is blank, a {@link DefaultAzureCredential}
 * is used.</p>
 */
final class ContainerClientFactory {

    static synchronized BlobContainerClient newContainerClient() {
        String containerName = getConfiguredContainerName();
        return newServiceClient().getBlobContainerClient(containerName);
    }

    private static BlobServiceClient newServiceClient() {
        String accountName = getConfiguredAccountName();
        if (accountName == null || accountName.isBlank()) {
            throw new IllegalArgumentException("Account name is not set");
        }

        final BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

        // The account key is not required to be set.
        String accountKey = getConfiguredAccountKey();
        if (accountKey.isBlank()) {
            DefaultAzureCredential credential = new DefaultAzureCredentialBuilder()
                    .executorService(VirtualThreadPool.getInstance().getExecutorService())
                    .build();
            builder.credential(credential);
        } else {
            StorageSharedKeyCredential credential =
                    new StorageSharedKeyCredential(accountName, accountKey);
            builder.credential(credential);
        }
        // The endpoint is also not required to be set.
        String endpoint = getConfiguredEndpoint();
        // If the endpoint is not set in the configuration, use the default
        // Microsoft endpoint.
        if (endpoint == null || endpoint.isBlank()) {
            endpoint = String.format("https://%s.blob.core.windows.net/",
                    accountName);
        }
        return builder.endpoint(endpoint).buildClient();
    }

    private static String getConfiguredAccountKey() {
        return Configuration.forApplication().
                getString(Key.AZUREBLOBSTORAGECACHE_ACCOUNT_KEY.key(), "");
    }

    private static String getConfiguredAccountName() {
        return Configuration.forApplication().
                getString(Key.AZUREBLOBSTORAGECACHE_ACCOUNT_NAME.key(), "");
    }

    static String getConfiguredContainerName() {
        // All letters in a container name must be lowercase.
        String containerName = Configuration.forApplication().
                getString(Key.AZUREBLOBSTORAGECACHE_CONTAINER_NAME.key(), "");
        return !containerName.isBlank() ? containerName.toLowerCase() : "";
    }

    private static String getConfiguredEndpoint() {
        Configuration config = Configuration.forApplication();
        String endpoint      = config.getString(Key.AZUREBLOBSTORAGECACHE_ENDPOINT.key(), "");
        if (!endpoint.isBlank()) {
            String accountName = config.getString(Key.AZUREBLOBSTORAGECACHE_ACCOUNT_NAME.key());
            return StringUtils.stripEnd(endpoint, "/") + "/" + accountName;
        }
        return null;
    }

    private ContainerClientFactory() {}

}

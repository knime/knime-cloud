/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Oct 30, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.utils.personalize;

import java.util.List;

import org.knime.cloud.core.util.port.CloudConnectionInformation;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.ListRolesRequest;
import com.amazonaws.services.identitymanagement.model.ListRolesResult;
import com.amazonaws.services.identitymanagement.model.Role;
import com.amazonaws.services.personalize.AmazonPersonalize;
import com.amazonaws.services.personalize.model.CampaignSummary;
import com.amazonaws.services.personalize.model.DatasetGroupSummary;
import com.amazonaws.services.personalize.model.DatasetSchemaSummary;
import com.amazonaws.services.personalize.model.ListCampaignsRequest;
import com.amazonaws.services.personalize.model.ListCampaignsResult;
import com.amazonaws.services.personalize.model.ListDatasetGroupsRequest;
import com.amazonaws.services.personalize.model.ListDatasetGroupsResult;
import com.amazonaws.services.personalize.model.ListRecipesRequest;
import com.amazonaws.services.personalize.model.ListRecipesResult;
import com.amazonaws.services.personalize.model.ListSchemasRequest;
import com.amazonaws.services.personalize.model.ListSchemasResult;
import com.amazonaws.services.personalize.model.ListSolutionVersionsRequest;
import com.amazonaws.services.personalize.model.ListSolutionVersionsResult;
import com.amazonaws.services.personalize.model.ListSolutionsRequest;
import com.amazonaws.services.personalize.model.ListSolutionsResult;
import com.amazonaws.services.personalize.model.RecipeSummary;
import com.amazonaws.services.personalize.model.SolutionSummary;
import com.amazonaws.services.personalize.model.SolutionVersionSummary;

/**
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
public class AmazonPersonalizeUtils {

    /**
     * Interface for a function that waits for a certain event to happen.
     */
    public interface WaitFunction {
        /**
         * @return true, if the execution can be continued
         */
        boolean continueExecution();
    }

    /**
     * @param f the function which checks if the execution can be continued
     * @param millis the time to wait until another check is done with the wait function
     * @throws InterruptedException
     */
    public static void waitUntilActive(final WaitFunction f, final int millis) throws InterruptedException {
        while (true) {
            Thread.sleep(millis);
            if (f.continueExecution()) {
                break;
            }
        }
    }

    /**
     * @param personalize the amazon personalize client
     * @return all dataset groups
     */
    public static List<DatasetGroupSummary> listAllDatasetGroups(final AmazonPersonalize personalize) {
        final ListDatasetGroupsRequest listDatasetGroupsRequest = new ListDatasetGroupsRequest().withMaxResults(100);
        ListDatasetGroupsResult listDatasetGroups = personalize.listDatasetGroups(listDatasetGroupsRequest);
        List<DatasetGroupSummary> datasetGroups = listDatasetGroups.getDatasetGroups();
        String nextToken;
        while ((nextToken = listDatasetGroups.getNextToken()) != null) {
            listDatasetGroups = personalize.listDatasetGroups(listDatasetGroupsRequest.withNextToken(nextToken));
            datasetGroups.addAll(listDatasetGroups.getDatasetGroups());
        }
        return datasetGroups;
    }

    /**
     * @param personalize the amazon personalize client
     * @return all recipes
     */
    public static List<RecipeSummary> listAllRecipes(final AmazonPersonalize personalize) {
        final ListRecipesRequest request = new ListRecipesRequest().withMaxResults(100);
        ListRecipesResult result = personalize.listRecipes(request);
        List<RecipeSummary> list = result.getRecipes();
        String nextToken;
        while ((nextToken = result.getNextToken()) != null) {
            result = personalize.listRecipes(request.withNextToken(nextToken));
            list.addAll(result.getRecipes());
        }
        return list;
    }

    /**
     * @param personalize the amazon personalize client
     * @return all schemas
     */
    public static List<DatasetSchemaSummary> listAllSchemas(final AmazonPersonalize personalize) {
        final ListSchemasRequest request = new ListSchemasRequest().withMaxResults(100);
        ListSchemasResult result = personalize.listSchemas(request);
        List<DatasetSchemaSummary> list = result.getSchemas();
        String nextToken;
        while ((nextToken = result.getNextToken()) != null) {
            result = personalize.listSchemas(request.withNextToken(nextToken));
            list.addAll(result.getSchemas());
        }
        return list;
    }

    /**
     * @param personalize the amazon personalize client
     * @return all solution versions
     */
    public static List<SolutionVersionSummary> listAllSolutionVersions(final AmazonPersonalize personalize) {
        final ListSolutionVersionsRequest request = new ListSolutionVersionsRequest().withMaxResults(100);
        ListSolutionVersionsResult result = personalize.listSolutionVersions(request);
        List<SolutionVersionSummary> list = result.getSolutionVersions();
        String nextToken;
        while ((nextToken = result.getNextToken()) != null) {
            result = personalize.listSolutionVersions(request.withNextToken(nextToken));
            list.addAll(result.getSolutionVersions());
        }
        return list;
    }

    /**
     * @param personalize the amazon personalize client
     * @return all solutions
     */
    public static List<SolutionSummary> listAllSolutions(final AmazonPersonalize personalize) {
        final ListSolutionsRequest request = new ListSolutionsRequest().withMaxResults(100);
        ListSolutionsResult result = personalize.listSolutions(request);
        List<SolutionSummary> list = result.getSolutions();
        String nextToken;
        while ((nextToken = result.getNextToken()) != null) {
            result = personalize.listSolutions(request.withNextToken(nextToken));
            list.addAll(result.getSolutions());
        }
        return list;
    }

    /**
     * @param personalize the amazon personalize client
     * @return all campaigns
     */
    public static List<CampaignSummary> listAllCampaigns(final AmazonPersonalize personalize) {
        final ListCampaignsRequest request = new ListCampaignsRequest().withMaxResults(100);
        ListCampaignsResult result = personalize.listCampaigns(request);
        List<CampaignSummary> list = result.getCampaigns();
        String nextToken;
        while ((nextToken = result.getNextToken()) != null) {
            result = personalize.listCampaigns(request.withNextToken(nextToken));
            list.addAll(result.getCampaigns());
        }
        return list;
    }

    /**
     * @param m_connectionInformation the cloud connection information
     * @return all dataset groups
     */
    public static List<Role> listAllRoles(final CloudConnectionInformation m_connectionInformation) {
        final ClientConfiguration clientConfig =
            new ClientConfiguration().withConnectionTimeout(m_connectionInformation.getTimeout());
        final AmazonIdentityManagement client = AmazonIdentityManagementClientBuilder.standard()
            .withClientConfiguration(clientConfig).withRegion(m_connectionInformation.getHost()).build();

        final ListRolesRequest listRolesRequest = new ListRolesRequest().withMaxItems(1000);
        ListRolesResult listRoles = client.listRoles(listRolesRequest);
        List<Role> roles = listRoles.getRoles();
        String nextToken;
        while ((nextToken = listRoles.getMarker()) != null) {
            System.out.println(roles.size());
            listRoles = client.listRoles(listRolesRequest.withMarker(nextToken));
            roles.addAll(listRoles.getRoles());
        }
        return roles;
    }

    /**
     * Enumeration of relevant states a job on Amazon Personalize can have.
     *
     * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
     */
    public enum Status {
            /** If the job succeeded and is active. */
            ACTIVE("ACTIVE"),
            /** If the job failed. */
            CREATED_FAILED("CREATE FAILED");

        private final String m_status;

        /**
         *
         */
        private Status(final String status) {
            m_status = status;
        }

        /**
         * @return the status
         */
        public String getStatus() {
            return m_status;
        }
    }
}

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
package org.knime.cloud.aws.mlservices.nodes.personalize.train;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.knime.cloud.aws.mlservices.personalize.AmazonPersonalizeConnection;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils.Status;
import org.knime.cloud.aws.mlservices.utils.personalize.NameArnPair;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.DoubleCell.DoubleCellFactory;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.def.StringCell.StringCellFactory;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.amazonaws.services.personalize.AmazonPersonalize;
import com.amazonaws.services.personalize.model.AutoMLConfig;
import com.amazonaws.services.personalize.model.CreateSolutionRequest;
import com.amazonaws.services.personalize.model.CreateSolutionVersionRequest;
import com.amazonaws.services.personalize.model.DescribeRecipeRequest;
import com.amazonaws.services.personalize.model.DescribeSolutionRequest;
import com.amazonaws.services.personalize.model.DescribeSolutionResult;
import com.amazonaws.services.personalize.model.DescribeSolutionVersionRequest;
import com.amazonaws.services.personalize.model.DescribeSolutionVersionResult;
import com.amazonaws.services.personalize.model.GetSolutionMetricsRequest;
import com.amazonaws.services.personalize.model.ListSolutionsRequest;
import com.amazonaws.services.personalize.model.ListSolutionsResult;
import com.amazonaws.services.personalize.model.SolutionConfig;

/**
 * Node model for Amazon Personalize solution version creator node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeCreateSolutionVersionNodeModel extends NodeModel {

    static final NameArnPair DEFAULT_PREDEFINED_RECIPE =
        NameArnPair.of("aws-hrnn", "arn:aws:personalize:::recipe/aws-hrnn");

    static final String[] METRICS = new String[]{"coverage", "mean_reciprocal_rank_at_25",
        "normalized_discounted_cumulative_gain_at_5", "normalized_discounted_cumulative_gain_at_10",
        "normalized_discounted_cumulative_gain_at_25", "precision_at_5", "precision_at_10", "precision_at_25"};

    static final List<NameArnPair> USER_PERSONALIZATION_RECIPES;

    static final List<NameArnPair> PERSONALIZED_RANKING_RECIPES;

    static final List<NameArnPair> RELATED_ITEMS_RECIPES;

    static final List<NameArnPair> AUTOML_RECIPES;

    // List of available recipes here p.51: https://docs.aws.amazon.com/personalize/latest/dg/personalize-dg.pdf
    static {
        USER_PERSONALIZATION_RECIPES = new ArrayList<>();
        USER_PERSONALIZATION_RECIPES.add(DEFAULT_PREDEFINED_RECIPE);
        final NameArnPair awshrnnmetadata =
            NameArnPair.of("aws-hrnn-metadata", "arn:aws:personalize:::recipe/aws-hrnn-metadata");
        USER_PERSONALIZATION_RECIPES.add(awshrnnmetadata);
        USER_PERSONALIZATION_RECIPES
            .add(NameArnPair.of("aws-hrnn-coldstart", "arn:aws:personalize:::recipe/aws-hrnn-coldstart"));
        USER_PERSONALIZATION_RECIPES
            .add(NameArnPair.of("aws-popularity-count", "arn:aws:personalize:::recipe/aws-popularity-count"));

        PERSONALIZED_RANKING_RECIPES = new ArrayList<>();
        PERSONALIZED_RANKING_RECIPES
            .add(NameArnPair.of("aws-personalized-ranking", "arn:aws:personalize:::recipe/aws-personalized-ranking"));

        RELATED_ITEMS_RECIPES = new ArrayList<>();
        RELATED_ITEMS_RECIPES.add(NameArnPair.of("aws-sims", "arn:aws:personalize:::recipe/aws-sims"));

        AUTOML_RECIPES = new ArrayList<>();
        AUTOML_RECIPES.add(DEFAULT_PREDEFINED_RECIPE);
        AUTOML_RECIPES.add(awshrnnmetadata);
    }

    private AmazonPersonalizeCreateSolutionVersionNodeSettings m_settings;

    AmazonPersonalizeCreateSolutionVersionNodeModel() {
        super(new PortType[]{AmazonConnectionInformationPortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (m_settings == null) {
            throw new InvalidSettingsException("The node must be configured.");
        }
        return new PortObjectSpec[]{createOutputSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final CloudConnectionInformation cxnInfo =
            ((AmazonConnectionInformationPortObject)inObjects[0]).getConnectionInformation();
        try (final AmazonPersonalizeConnection personalizeConnection = new AmazonPersonalizeConnection(cxnInfo)) {
            final AmazonPersonalize personalizeClient = personalizeConnection.getClient();

            // Create solution configuration or use existing one
            final String solutionArn;
            if (m_settings.isCreateNewSolution()) {
                solutionArn = createSolution(personalizeClient);
            } else {
                solutionArn = m_settings.getExistingSolution().getARN();
            }

            // Wait until the solution is active
            final DescribeSolutionRequest describeSolutionRequest =
                new DescribeSolutionRequest().withSolutionArn(solutionArn);
            AmazonPersonalizeUtils.waitUntilActive(() -> {
                DescribeSolutionResult describeSolution = personalizeClient.describeSolution(describeSolutionRequest);
                final String status = describeSolution.getSolution().getStatus();
                exec.setMessage("Creating solution configuration (Status: " + status + ")");
                return status.equals(Status.ACTIVE.getStatus());
            }, 100);
            exec.setProgress(0.5);

            // Create solution version
            final String solutionVersionArn =
                personalizeClient.createSolutionVersion(new CreateSolutionVersionRequest().withSolutionArn(solutionArn))
                    .getSolutionVersionArn();

            // Wait until solution version is active (or failed)
            final DescribeSolutionVersionRequest describeSolutionVersionRequest =
                new DescribeSolutionVersionRequest().withSolutionVersionArn(solutionVersionArn);
            AmazonPersonalizeUtils.waitUntilActive(() -> {
                final DescribeSolutionVersionResult solutionVersionDescription =
                    personalizeClient.describeSolutionVersion(describeSolutionVersionRequest);
                final String status = solutionVersionDescription.getSolutionVersion().getStatus();
                exec.setMessage("Creating solution version (Status: " + status + ")");
                if (status.equals(Status.CREATED_FAILED.getStatus())) {
                    throw new IllegalStateException("No solution version has been created. Reason: "
                        + solutionVersionDescription.getSolutionVersion().getFailureReason());
                }
                return status.equals(Status.ACTIVE.getStatus());
            }, 2000);

            // Retrieve the recipe type to put it into the output
            final DescribeSolutionVersionResult solutionVersionDescription =
                personalizeClient.describeSolutionVersion(describeSolutionVersionRequest);
            final String recipeType = personalizeClient
                .describeRecipe(new DescribeRecipeRequest()
                    .withRecipeArn(solutionVersionDescription.getSolutionVersion().getRecipeArn()))
                .getRecipe().getRecipeType();
            //            final String recipeType = personalizeClient.describeRecipe(new DescribeRecipeRequest().withRecipeArn(
            //                personalizeClient.describeSolution(new DescribeSolutionRequest().withSolutionArn(solutionArn))
            //                    .getSolution().getRecipeArn()))
            //                .getRecipe().getRecipeType();

            // Create output
            final Map<String, Double> metrics = personalizeClient
                .getSolutionMetrics(new GetSolutionMetricsRequest().withSolutionVersionArn(solutionVersionArn))
                .getMetrics();
            if (m_settings.isOutputSolutionVersionArnAsVar()) {
                pushFlowVariableString("solution-version-ARN", solutionVersionArn);
            }
            return new PortObject[]{createOutput(exec, solutionVersionArn, recipeType, metrics)};
        }
    }

    private String createSolution(final AmazonPersonalize personalizeClient) {
        final NameArnPair datasetGroup = m_settings.getDatasetGroup();
        final RecipeSelection recipeSelection = m_settings.getRecipeSelection();
        final boolean isHPO = m_settings.isHyperparameterOpt();

        // Create the request
        final CreateSolutionRequest createSolutionRequest =
            new CreateSolutionRequest().withDatasetGroupArn(datasetGroup.getARN()).withPerformHPO(isHPO);
        if (recipeSelection == RecipeSelection.PREDEFINED) {
            createSolutionRequest.setPerformAutoML(false);
            createSolutionRequest.setRecipeArn(m_settings.getPredefinedRecipe().getARN());
        } else if (recipeSelection == RecipeSelection.USER_DEFINED) {
            createSolutionRequest.setPerformAutoML(false);
            createSolutionRequest.setRecipeArn(m_settings.getUserDefinedRecipeArn());
        } else if (recipeSelection == RecipeSelection.AUTOML) {
            createSolutionRequest.setPerformAutoML(true);
            final SolutionConfig solutionConfig = new SolutionConfig();
            solutionConfig.setAutoMLConfig(new AutoMLConfig()
                .withRecipeList(AUTOML_RECIPES.stream().map(e -> e.getARN()).collect(Collectors.toList())));
            createSolutionRequest.setSolutionConfig(solutionConfig);
        } else {
            throw new IllegalStateException("Unexpected recipe selection: " + recipeSelection.name());
        }

        final ListSolutionsResult solutions =
            personalizeClient.listSolutions(new ListSolutionsRequest().withDatasetGroupArn(datasetGroup.getARN()));
        final String solutionName = m_settings.getSolutionName();
        final Optional<String> optionalSolutionArn = solutions.getSolutions().stream()
            .filter(e -> e.getName().equals(solutionName)).map(e -> e.getSolutionArn()).findFirst();
        if (optionalSolutionArn.isPresent()) {
            throw new IllegalStateException("A solution with the name '" + solutionName + "' already exists.");
        }
        createSolutionRequest.setName(solutionName);
        return personalizeClient.createSolution(createSolutionRequest).getSolutionArn();
    }

    private static DataTableSpec createOutputSpec() {
        final DataColumnSpec[] colSpecs = new DataColumnSpec[METRICS.length + 2];
        colSpecs[0] = new DataColumnSpecCreator("Solution version ARN", StringCell.TYPE).createSpec();
        colSpecs[1] = new DataColumnSpecCreator("Recipe type", StringCell.TYPE).createSpec();
        for (int i = 0; i < METRICS.length; i++) {
            colSpecs[i + 2] = new DataColumnSpecCreator(METRICS[i], DoubleCell.TYPE).createSpec();
        }
        return new DataTableSpec(colSpecs);
    }

    private static BufferedDataTable createOutput(final ExecutionContext exec, final String solutionVersionArn,
        final String recipeType, final Map<String, Double> metrics) {
        final BufferedDataContainer container = exec.createDataContainer(createOutputSpec());
        final DataCell[] cells = new DataCell[METRICS.length + 2];
        cells[0] = StringCellFactory.create(solutionVersionArn);
        cells[1] = StringCellFactory.create(recipeType);
        for (int i = 0; i < METRICS.length; i++) {
            cells[i + 2] = DoubleCellFactory.create(metrics.get(METRICS[i]));
        }
        container.addRowToTable(new DefaultRow("Values", cells));
        container.close();
        return container.getTable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_settings != null) {
            m_settings.saveSettings(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (m_settings == null) {
            m_settings = new AmazonPersonalizeCreateSolutionVersionNodeSettings();
        }
        m_settings.loadSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to do
    }

}

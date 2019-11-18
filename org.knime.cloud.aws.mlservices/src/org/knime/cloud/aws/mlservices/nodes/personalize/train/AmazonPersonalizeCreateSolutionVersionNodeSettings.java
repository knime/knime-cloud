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

import org.knime.cloud.aws.mlservices.utils.personalize.NameArnPair;
import org.knime.cloud.aws.mlservices.utils.personalize.RecipeType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Node settings for Amazon Personalize solution version creator node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeCreateSolutionVersionNodeSettings {

    private static final String CFG_KEY_CREATE_NEW_SOLUTION = "create_new_solution";

    private static final String CFG_KEY_DATASET_GROUP = "dataset_group";

    private static final String CFG_KEY_RECIPE_SELECTION = "recipe_selection";

    private static final String CFG_KEY_PREDEFINED_RECIPE = "predefined_recipe";

    private static final String CFG_KEY_PREDEFINED_RECIPE_TYPE = "predefined_recipe_type";

    private static final String CFG_KEY_USER_DEFINED_RECIPE_ARN = "user_defined_recipe_arn";

    private static final String CFG_KEY_HYPERPARAMETER_OPT = "hyperparameter_opt";

    private static final String CFG_KEY_SOLUTION_NAME = "solution_name";

    private static final String CFG_KEY_OUTPUT_ARN_AS_VARIABLE = "output_solution_version_arn_as_variable";

    private static final String CFG_KEY_EXISTING_SOLUTION = "existing_solution";

    private static final boolean DEF_CREATE_NEW_SOLUTION = true;

    private static final NameArnPair DEF_DATASET_GROUP = null;

    private static final RecipeSelection DEF_RECIPE_SELECTION = RecipeSelection.PREDEFINED;

    private static final NameArnPair DEF_PREDEFINED_RECIPE = null;

    private static final RecipeType DEF_PREDEFINED_RECIPE_TYPE = RecipeType.USER_PERSONALIZATION;

    private static final String DEF_USER_DEFINED_RECIPE_ARN = "";

    private static final boolean DEF_HYPERPARAMETER_OPT = false;

    private static final String DEF_SOLUTION_NAME = "new-solution";

    private static final boolean DEF_OUTPUT_ARN_AS_VARIABLE = false;

    private static final NameArnPair DEF_EXISTING_SOLUTION = null;

    private boolean m_createNewSolution = DEF_CREATE_NEW_SOLUTION;

    private NameArnPair m_datasetGroup = DEF_DATASET_GROUP;

    private RecipeSelection m_recipeSelection = DEF_RECIPE_SELECTION;

    private NameArnPair m_predefinedRecipe = DEF_PREDEFINED_RECIPE;

    private RecipeType m_predefinedRecipeType = DEF_PREDEFINED_RECIPE_TYPE;

    private String m_userDefinedRecipeArn = DEF_USER_DEFINED_RECIPE_ARN;

    private boolean m_hyperparameterOpt = DEF_HYPERPARAMETER_OPT;

    private String m_solutionName = DEF_SOLUTION_NAME;

    private boolean m_outputSolutionVersionArnAsVar = DEF_OUTPUT_ARN_AS_VARIABLE;

    private NameArnPair m_existingSolution = DEF_EXISTING_SOLUTION;

    /**
     * @return the datasetGroup
     */
    public NameArnPair getDatasetGroup() {
        return m_datasetGroup;
    }

    /**
     * @param datasetGroup the datasetGroup to set
     */
    public void setDatasetGroup(final NameArnPair datasetGroup) {
        m_datasetGroup = datasetGroup;
    }

    /**
     * @return the recipeSelection
     */
    public RecipeSelection getRecipeSelection() {
        return m_recipeSelection;
    }

    /**
     * @param recipeSelection the recipeSelection to set
     */
    public void setRecipeSelection(final RecipeSelection recipeSelection) {
        m_recipeSelection = recipeSelection;
    }

    /**
     * @return the predefinedRecipe
     */
    public NameArnPair getPredefinedRecipe() {
        return m_predefinedRecipe;
    }

    /**
     * @param predefinedRecipe the predefinedRecipe to set
     */
    public void setPredefinedRecipe(final NameArnPair predefinedRecipe) {
        m_predefinedRecipe = predefinedRecipe;
    }

    /**
     * @return the predefinedRecipeType
     */
    public RecipeType getPredefinedRecipeType() {
        return m_predefinedRecipeType;
    }

    /**
     * @param predefinedRecipeType the predefinedRecipeType to set
     */
    public void setPredefinedRecipeType(final RecipeType predefinedRecipeType) {
        m_predefinedRecipeType = predefinedRecipeType;
    }

    /**
     * @return the userDefinedRecipeArn
     */
    public String getUserDefinedRecipeArn() {
        return m_userDefinedRecipeArn;
    }

    /**
     * @param userDefinedRecipeArn the userDefinedRecipeArn to set
     * @throws InvalidSettingsException if the arn is not valid
     */
    public void setUserDefinedRecipeArn(final String userDefinedRecipeArn) throws InvalidSettingsException {
        if (m_recipeSelection == RecipeSelection.USER_DEFINED
            && !userDefinedRecipeArn.startsWith("arn:aws:personalize:")) {
            throw new InvalidSettingsException("Please enter a valid recipe arn.");
        }
        m_userDefinedRecipeArn = userDefinedRecipeArn;
    }

    /**
     * @return the hyperparameterOpt
     */
    public boolean isHyperparameterOpt() {
        return m_hyperparameterOpt;
    }

    /**
     * @param hyperparameterOpt the hyperparameterOpt to set
     */
    public void setHyperparameterOpt(final boolean hyperparameterOpt) {
        m_hyperparameterOpt = hyperparameterOpt;
    }

    /**
     * @return the solutionName
     */
    public String getSolutionName() {
        return m_solutionName;
    }

    /**
     * @param solutionName the solutionName to set
     */
    public void setSolutionName(final String solutionName) {
        m_solutionName = solutionName;
    }

    /**
     * @return the outputSolutionVersionArnAsVar
     */
    public boolean isOutputSolutionVersionArnAsVar() {
        return m_outputSolutionVersionArnAsVar;
    }

    /**
     * @param outputSolutionVersionArnAsVar the outputSolutionVersionArnAsVar to set
     */
    public void setOutputSolutionVersionArnAsVar(final boolean outputSolutionVersionArnAsVar) {
        m_outputSolutionVersionArnAsVar = outputSolutionVersionArnAsVar;
    }

    /**
     * @return the createNewSolution
     */
    public boolean isCreateNewSolution() {
        return m_createNewSolution;
    }

    /**
     * @param createNewSolution the createNewSolution to set
     */
    public void setCreateNewSolution(final boolean createNewSolution) {
        m_createNewSolution = createNewSolution;
    }

    /**
     * @return the existingSolution
     */
    public NameArnPair getExistingSolution() {
        return m_existingSolution;
    }

    /**
     * @param existingSolution the existingSolution to set
     */
    public void setExistingSolution(final NameArnPair existingSolution) {
        m_existingSolution = existingSolution;
    }

    /**
     * Loads the settings from the node settings object.
     *
     * @param settings a node settings object
     * @throws InvalidSettingsException if some settings are missing
     */
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_createNewSolution = settings.getBoolean(CFG_KEY_CREATE_NEW_SOLUTION);
        m_datasetGroup = NameArnPair.loadSettings(settings, CFG_KEY_DATASET_GROUP);
        m_predefinedRecipe = NameArnPair.loadSettings(settings, CFG_KEY_PREDEFINED_RECIPE);
        m_recipeSelection = RecipeSelection.valueOf(settings.getString(CFG_KEY_RECIPE_SELECTION));
        m_predefinedRecipeType = RecipeType.valueOf(settings.getString(CFG_KEY_PREDEFINED_RECIPE_TYPE));
        m_userDefinedRecipeArn = settings.getString(CFG_KEY_USER_DEFINED_RECIPE_ARN);
        m_hyperparameterOpt = settings.getBoolean(CFG_KEY_HYPERPARAMETER_OPT);
        m_solutionName = settings.getString(CFG_KEY_SOLUTION_NAME);
        m_outputSolutionVersionArnAsVar = settings.getBoolean(CFG_KEY_OUTPUT_ARN_AS_VARIABLE);
        m_existingSolution = NameArnPair.loadSettings(settings, CFG_KEY_EXISTING_SOLUTION);
    }

    /**
     * Loads the settings from the node settings object using default values if some settings are missing.
     *
     * @param settings a node settings object
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        m_createNewSolution = settings.getBoolean(CFG_KEY_CREATE_NEW_SOLUTION, DEF_CREATE_NEW_SOLUTION);
        m_datasetGroup = NameArnPair.loadSettingsForDialog(settings, CFG_KEY_DATASET_GROUP, DEF_DATASET_GROUP);
        m_predefinedRecipe =
            NameArnPair.loadSettingsForDialog(settings, CFG_KEY_PREDEFINED_RECIPE, DEF_PREDEFINED_RECIPE);
        m_recipeSelection =
            RecipeSelection.valueOf(settings.getString(CFG_KEY_RECIPE_SELECTION, DEF_RECIPE_SELECTION.name()));
        m_predefinedRecipeType =
            RecipeType.valueOf(settings.getString(CFG_KEY_PREDEFINED_RECIPE_TYPE, DEF_PREDEFINED_RECIPE_TYPE.name()));
        m_userDefinedRecipeArn = settings.getString(CFG_KEY_USER_DEFINED_RECIPE_ARN, DEF_USER_DEFINED_RECIPE_ARN);
        m_hyperparameterOpt = settings.getBoolean(CFG_KEY_HYPERPARAMETER_OPT, DEF_HYPERPARAMETER_OPT);
        m_solutionName = settings.getString(CFG_KEY_SOLUTION_NAME, DEF_SOLUTION_NAME);
        m_outputSolutionVersionArnAsVar =
            settings.getBoolean(CFG_KEY_OUTPUT_ARN_AS_VARIABLE, DEF_OUTPUT_ARN_AS_VARIABLE);
        m_existingSolution =
            NameArnPair.loadSettingsForDialog(settings, CFG_KEY_EXISTING_SOLUTION, DEF_EXISTING_SOLUTION);
    }

    /**
     * Saves the settings into the node settings object.
     *
     * @param settings a node settings object
     */
    public void saveSettings(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_KEY_CREATE_NEW_SOLUTION, m_createNewSolution);
        if (m_datasetGroup != null) {
            m_datasetGroup.saveSettings(settings, CFG_KEY_DATASET_GROUP);
        }
        settings.addString(CFG_KEY_RECIPE_SELECTION, m_recipeSelection.name());
        settings.addString(CFG_KEY_PREDEFINED_RECIPE_TYPE, m_predefinedRecipeType.name());
        if (m_predefinedRecipe != null) {
            m_predefinedRecipe.saveSettings(settings, CFG_KEY_PREDEFINED_RECIPE);
        }
        settings.addString(CFG_KEY_USER_DEFINED_RECIPE_ARN, m_userDefinedRecipeArn);
        settings.addBoolean(CFG_KEY_HYPERPARAMETER_OPT, m_hyperparameterOpt);
        settings.addString(CFG_KEY_SOLUTION_NAME, m_solutionName);
        settings.addBoolean(CFG_KEY_OUTPUT_ARN_AS_VARIABLE, m_outputSolutionVersionArnAsVar);
        if (m_predefinedRecipe != null) {
            m_predefinedRecipe.saveSettings(settings, CFG_KEY_PREDEFINED_RECIPE);
        }
        if (m_existingSolution != null) {
            m_existingSolution.saveSettings(settings, CFG_KEY_EXISTING_SOLUTION);
        }
    }

}

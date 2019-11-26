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
 *   Oct 29, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.upload;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.filter.InputFilter;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

import com.amazonaws.util.StringUtils;

/**
 * Abstract node settings for Amazon Personalize data upload nodes.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractAmazonPersonalizeDataUploadNodeSettings {

    private static final String CFG_KEY_TARGET = "target";

    private static final String CFG_KEY_SELECTED_DATASET_GROUP = "selected_dataset_group";

    private static final String CFG_KEY_DATASET_NAME = "dataset_name";

    private static final String CFG_KEY_OVERWRITE_DATASET_POLICY = "overwrite_dataset_policy";

    private static final String CFG_KEY_PREFIX_IMPORT_JOB_NAME = "prefix_import_job_name";

    private static final String CFG_KEY_PREFIX_SCHEMA_NAME = "prefix_schema_name";

    private static final String CFG_KEY_IAM_SERVICE_ROLE_ARN = "iam_service_role_arn";

    private static final String CFG_KEY_METADATA_COLUMN_FILTER = "column_filter";

    private static final String DEF_TARGET = null;

    private static final String DEF_SELECTED_DATASET_GROUP = "new-dataset-group";

    private static final String DEF_OVERWRITE_DATASET_POLICY = OverwritePolicy.ABORT.toString();

    private static final String DEF_IAM_SERVICE_ROLE_ARN = "";

    private final String DEF_DATASET_NAME = StringUtils.lowerCase(getDatasetType()) + "-data";

    private final String DEF_PREFIX_IMPORT_JOB_NAME =
        "KNIME-import-" + StringUtils.lowerCase(getDatasetType()) + "-dataset-job";

    private final String DEF_PREFIX_SCHEMA_NAME = "KNIME-schema-" + StringUtils.lowerCase(getDatasetType());

    private String m_target = DEF_TARGET;

    private String m_selectedDatasetGroup = DEF_SELECTED_DATASET_GROUP;

    private String m_datasetName = DEF_DATASET_NAME;

    private String m_overwriteDatasetPolicy = DEF_OVERWRITE_DATASET_POLICY;

    private String m_prefixImportJobName = DEF_PREFIX_IMPORT_JOB_NAME;

    private String m_prefixSchemaName = DEF_PREFIX_SCHEMA_NAME;

    private String m_iamServiceRoleArn = DEF_IAM_SERVICE_ROLE_ARN;

    private DataColumnSpecFilterConfiguration m_filterConfig =
        new DataColumnSpecFilterConfiguration(CFG_KEY_METADATA_COLUMN_FILTER, new InputFilter<DataColumnSpec>() {

            @Override
            public boolean include(final DataColumnSpec colSpec) {
                final DataType type = colSpec.getType();
                return type.isCompatible(StringValue.class) || type.isCompatible(DoubleValue.class);
            }
        });

    /**
     * @return the target
     */
    public String getTarget() {
        return m_target;
    }

    /**
     * @param target the target to set
     * @throws InvalidSettingsException if target is empty
     */
    public void setTarget(final String target) throws InvalidSettingsException {
        if (target.trim().isEmpty()) {
            throw new InvalidSettingsException("Target must not be empty.");
        }
        m_target = target;
    }

    /**
     * @return the selectedDatasetGroup
     */
    public String getSelectedDatasetGroup() {
        return m_selectedDatasetGroup;
    }

    /**
     * @param selectedDatasetGroup the selectedDatasetGroup to set
     * @throws InvalidSettingsException if the dataset group name is empty or has more than 63 characters
     */
    public void setSelectedDatasetGroup(final String selectedDatasetGroup) throws InvalidSettingsException {
        if (selectedDatasetGroup.trim().isEmpty()) {
            throw new InvalidSettingsException("The dataset group name must not be empty.");
        }
        if (selectedDatasetGroup.length() > 63) {
            throw new InvalidSettingsException("The dataset group name must have at most 63 characters, but has "
                + selectedDatasetGroup.length() + ".");
        }
        m_selectedDatasetGroup = selectedDatasetGroup;
    }

    /**
     * @return the datasetName
     */
    public String getDatasetName() {
        return m_datasetName;
    }

    /**
     * @param datasetName the datasetName to set
     * @throws InvalidSettingsException if the dataset name is empty
     */
    public void setDatasetName(final String datasetName) throws InvalidSettingsException {
        if (datasetName.trim().isEmpty()) {
            throw new InvalidSettingsException("The dataset name must not be empty.");
        }
        m_datasetName = datasetName;
    }

    /**
     * @return the overwriteDatasetPolicy
     */
    public String getOverwriteDatasetPolicy() {
        return m_overwriteDatasetPolicy;
    }

    /**
     * @param overwriteDatasetPolicy the overwriteDatasetPolicy to set
     */
    public void setOverwriteDatasetPolicy(final String overwriteDatasetPolicy) {
        m_overwriteDatasetPolicy = overwriteDatasetPolicy;
    }

    /**
     * @return the filterConfig
     */
    public DataColumnSpecFilterConfiguration getFilterConfig() {
        return m_filterConfig;
    }

    /**
     * @param filterConfig the filterConfig to set
     */
    public void setFilterConfig(final DataColumnSpecFilterConfiguration filterConfig) {
        m_filterConfig = filterConfig;
    }

    /**
     * @return the prefixImportJobName
     */
    public String getPrefixImportJobName() {
        return m_prefixImportJobName;
    }

    /**
     * @param prefixImportJobName the prefixImportJobName to set
     */
    public void setPrefixImportJobName(final String prefixImportJobName) {
        m_prefixImportJobName = prefixImportJobName;
    }

    /**
     * @return the iamServiceRoleArn
     */
    public String getIamServiceRoleArn() {
        return m_iamServiceRoleArn;
    }

    /**
     * @param iamServiceRoleArn the iamServiceRoleArn to set
     * @throws InvalidSettingsException if iamServiceRoleArn is empty
     */
    public void setIamServiceRoleArn(final String iamServiceRoleArn) throws InvalidSettingsException {
        if (!iamServiceRoleArn.startsWith("arn:aws:iam:")) {
            throw new InvalidSettingsException("Please enter a valid IAM role arn.");
        }
        m_iamServiceRoleArn = iamServiceRoleArn;
    }

    /**
     * @return the prefixSchemaName
     */
    public String getPrefixSchemaName() {
        return m_prefixSchemaName;
    }

    /**
     * @param prefixSchemaName the prefixSchemaName to set
     */
    public void setPrefixSchemaName(final String prefixSchemaName) {
        m_prefixSchemaName = prefixSchemaName;
    }

    /**
     * Loads the settings from the node settings object.
     *
     * @param settings a node settings object
     * @throws InvalidSettingsException if some settings are missing
     */
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_target = settings.getString(CFG_KEY_TARGET);
        m_selectedDatasetGroup = settings.getString(CFG_KEY_SELECTED_DATASET_GROUP);
        m_datasetName = settings.getString(CFG_KEY_DATASET_NAME);
        m_overwriteDatasetPolicy = settings.getString(CFG_KEY_OVERWRITE_DATASET_POLICY);
        m_prefixImportJobName = settings.getString(CFG_KEY_PREFIX_IMPORT_JOB_NAME);
        m_prefixSchemaName = settings.getString(CFG_KEY_PREFIX_SCHEMA_NAME);
        m_iamServiceRoleArn = settings.getString(CFG_KEY_IAM_SERVICE_ROLE_ARN);
        m_filterConfig.loadConfigurationInModel(settings);
    }

    /**
     * Loads the settings from the node settings object using default values if some settings are missing.
     *
     * @param settings a node settings object
     * @param spec the input spec
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings, final DataTableSpec spec) {
        m_target = settings.getString(CFG_KEY_TARGET, DEF_TARGET);
        m_selectedDatasetGroup = settings.getString(CFG_KEY_SELECTED_DATASET_GROUP, DEF_SELECTED_DATASET_GROUP);
        m_datasetName = settings.getString(CFG_KEY_DATASET_NAME, DEF_DATASET_NAME);
        m_overwriteDatasetPolicy = settings.getString(CFG_KEY_OVERWRITE_DATASET_POLICY, DEF_OVERWRITE_DATASET_POLICY);
        m_prefixImportJobName = settings.getString(CFG_KEY_PREFIX_IMPORT_JOB_NAME, DEF_PREFIX_IMPORT_JOB_NAME);
        m_prefixSchemaName = settings.getString(CFG_KEY_PREFIX_SCHEMA_NAME, DEF_PREFIX_SCHEMA_NAME);
        m_iamServiceRoleArn = settings.getString(CFG_KEY_IAM_SERVICE_ROLE_ARN, DEF_IAM_SERVICE_ROLE_ARN);
        m_filterConfig.loadConfigurationInDialog(settings, spec);
    }

    /**
     * Saves the settings into the node settings object.
     *
     * @param settings a node settings object
     */
    public void saveSettings(final NodeSettingsWO settings) {
        settings.addString(CFG_KEY_TARGET, m_target);
        settings.addString(CFG_KEY_SELECTED_DATASET_GROUP, m_selectedDatasetGroup);
        settings.addString(CFG_KEY_DATASET_NAME, m_datasetName);
        settings.addString(CFG_KEY_OVERWRITE_DATASET_POLICY, m_overwriteDatasetPolicy);
        settings.addString(CFG_KEY_PREFIX_IMPORT_JOB_NAME, m_prefixImportJobName);
        settings.addString(CFG_KEY_PREFIX_SCHEMA_NAME, m_prefixSchemaName);
        settings.addString(CFG_KEY_IAM_SERVICE_ROLE_ARN, m_iamServiceRoleArn);
        m_filterConfig.saveConfiguration(settings);
    }

    /**
     * @return the dataset type that should be uploaded
     */
    protected abstract String getDatasetType();

    /**
     * @return the number of selected optional columns (usually 0)
     */
    protected int getNumSelectedOptionalColumns() {
        return 0;
    }

    /**
     * @return the number of required columns
     */
    protected abstract int getNumRequiredColumns();

    /**
     * @return the minimum number of metadata columns
     */
    protected abstract int getMinMetadataColumns();

    /**
     * @return the maximum number of metadata columns
     */
    protected int getMaxMetadataColumns() {
        return 5;
    }

}

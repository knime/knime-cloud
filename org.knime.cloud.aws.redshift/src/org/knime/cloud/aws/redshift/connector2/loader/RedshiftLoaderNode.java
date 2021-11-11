/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */
package org.knime.cloud.aws.redshift.connector2.loader;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

import org.eclipse.core.runtime.URIUtil;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.agent.loader.DBLoaderMode;
import org.knime.database.model.DBTable;
import org.knime.database.node.component.PreferredHeightPanel;
import org.knime.database.node.io.load.DBLoaderNode2;
import org.knime.database.node.io.load.DBLoaderNode2Factory;
import org.knime.database.node.io.load.ExecutionParameters;
import org.knime.database.node.io.load.impl.fs.ConnectedLoaderNode2;
import org.knime.database.node.io.load.impl.fs.util.DBFileWriter;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBPortObject;
import org.knime.database.session.DBSession;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;

/**
 * Implementation of the loader node for the Redshift database.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class RedshiftLoaderNode
extends ConnectedLoaderNode2<RedshiftLoaderNodeComponents, RedshiftLoaderNodeSettings>
implements DBLoaderNode2Factory<RedshiftLoaderNodeComponents, RedshiftLoaderNodeSettings> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(RedshiftLoaderNode.class);

    private static final List<Charset> CHARSETS = unmodifiableList(asList(StandardCharsets.UTF_8,
        StandardCharsets.UTF_16, StandardCharsets.UTF_16BE, StandardCharsets.UTF_16LE));

    private static Box createBox(final boolean horizontal) {
        final Box box;
        if (horizontal) {
            box = new Box(BoxLayout.X_AXIS);
        } else {
            box = new Box(BoxLayout.Y_AXIS);
        }
        return box;
    }

    private static JPanel createPanel() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        return panel;
    }

    private void onFileFormatSelectionChange(final RedshiftLoaderNodeComponents components) {
        final Optional<RedshiftLoaderFileFormat> optionalFileFormat =
            RedshiftLoaderFileFormat.optionalValueOf(components.getFileFormatSelectionModel().getStringValue());
        final RedshiftLoaderFileFormat fileFormat;
        if (optionalFileFormat.isEmpty()) {
            //this can happen only during the first loading of the node set the default file format
            //this is necessary to not overwrite any user entered values for chunk and file size
            fileFormat = RedshiftLoaderFileFormat.getDefault();
            m_init = false;
            components.getFileFormatSelectionModel().setStringValue(fileFormat.getActionCommand());
        } else {
            fileFormat = optionalFileFormat.get();
        }
        if (!m_init) {
            //the format has truly changed by the user so we need to update the default sizes
            components.getChunkSizeModel().setIntValue(fileFormat.getDefaultChunkSize());
            components.getFileSizeModel().setLongValue(fileFormat.getDefaultFileSize());
        }

        final List<String> compressionFormats = fileFormat.getCompressionFormats();
        components.getCompressionComponent().replaceListItems(compressionFormats,
            components.getCompressionModel().getStringValue());

        components.getChunkSizeComponent().setToolTipText(fileFormat.getChunkSizeToolTipText());
        components.getFileSizeComponent().setToolTipText(fileFormat.getFileSizeToolTipText());

        final boolean isCSV =
            optionalFileFormat.isPresent() && optionalFileFormat.get() == RedshiftLoaderFileFormat.CSV;
        components.getFileFormatModel().setEnabled(isCSV);
        components.getChunkSizeModel().setEnabled(!isCSV);
        components.getFileSizeModel().setEnabled(!isCSV);
    }

    private boolean m_init = false;

    @Override
    public DBLoaderNode2<RedshiftLoaderNodeComponents, RedshiftLoaderNodeSettings> get() {
        return new RedshiftLoaderNode();
    }

    @Override
    public void buildDialog(final DialogBuilder builder, final List<DialogComponent> dialogComponents,
        final RedshiftLoaderNodeComponents customComponents) {
        builder.addTab(Integer.MAX_VALUE, "Options", createOptionsPanel(customComponents), true);
        builder.addTab(Integer.MAX_VALUE, "Advanced", createAdvancedPanel(customComponents), true);

        customComponents.getFileFormatSelectionModel()
            .addChangeListener(event -> onFileFormatSelectionChange(customComponents));
    }

    private static JPanel createOptionsPanel(final RedshiftLoaderNodeComponents customComponents) {
        final JPanel optionsPanel = createTargetTableFolderPanel(customComponents);
        optionsPanel.add(fileFormatPanel(customComponents));
        optionsPanel.add(customComponents.getAuthorizationComponent().getComponentPanel());
        return optionsPanel;
    }

    private static JPanel createAdvancedPanel(final RedshiftLoaderNodeComponents cc) {
        final JPanel advancedPanel = createPanel();
        final Box advancedBox = createBox(false);
        final JPanel generalPanel = createPanel();
        generalPanel.setBorder(BorderFactory.createTitledBorder(" General Settings "));
        generalPanel.add(cc.getCompressionComponent().getComponentPanel());
        generalPanel.add(cc.getCompressionComponent().getComponentPanel());
        advancedBox.add(generalPanel);
        final JPanel csvPanel = cc.getFileFormatComponent().getComponentPanel();
        csvPanel.setBorder(BorderFactory.createTitledBorder(" CSV Settings "));
        advancedBox.add(csvPanel);
        final JPanel orcParquetPanel = createPanel();
        orcParquetPanel .setBorder(BorderFactory.createTitledBorder(" ORC/Parquet Settings "));
        orcParquetPanel.add(cc.getChunkSizeComponent().getComponentPanel());
        orcParquetPanel.add(cc.getFileSizeComponent().getComponentPanel());
        advancedBox.add(orcParquetPanel);
        advancedPanel.add(advancedBox);
        return advancedPanel;
    }

    private static JPanel fileFormatPanel(final RedshiftLoaderNodeComponents redshiftCustomComponents) {
        final JPanel fileFormatPanel = new PreferredHeightPanel(new GridBagLayout());
        fileFormatPanel.setBorder(new TitledBorder("File format"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 0;
        fileFormatPanel.add(redshiftCustomComponents.getFileFormatSelectionComponent().getComponentPanel(), gbc);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        gbc.gridx++;
        fileFormatPanel.add(new JLabel(), gbc);
        return fileFormatPanel;
    }

    @Override
    public DBDataPortObjectSpec configureModel(final PortObjectSpec[] inSpecs, final List<SettingsModel> settingsModels,
        final RedshiftLoaderNodeSettings customSettings) throws InvalidSettingsException {
        final DBPortObject sessionPortObjectSpec = getDBSpec(inSpecs);
        validateColumns(false, createModelConfigurationExecutionMonitor(sessionPortObjectSpec.getDBSession()),
            getDataSpec(inSpecs), sessionPortObjectSpec, customSettings.getTableNameModel().toDBTable());
        return super.configureModel(inSpecs, settingsModels, customSettings);
    }

    @Override
    public RedshiftLoaderNodeComponents createCustomDialogComponents(final DialogDelegate dialogDelegate) {
        return new RedshiftLoaderNodeComponents(dialogDelegate, CHARSETS);
    }

    @Override
    public RedshiftLoaderNodeSettings createCustomModelSettings(final ModelDelegate modelDelegate) {
        return new RedshiftLoaderNodeSettings(modelDelegate);
    }

    @Override
    public List<DialogComponent> createDialogComponents(final RedshiftLoaderNodeComponents cc) {
        return asList(cc.getTargetFolderComponent(), cc.getTableNameComponent(), cc.getFileFormatSelectionComponent(),
            cc.getAuthorizationComponent(), cc.getFileFormatComponent(), cc.getCompressionComponent(),
            cc.getChunkSizeComponent(), cc.getFileSizeComponent());
    }

    @Override
    public void onCloseInDialog(final RedshiftLoaderNodeComponents customComponents) {
        super.onCloseInDialog(customComponents);
        customComponents.getTargetFolderComponent().onClose();
    }

    @Override
    public List<SettingsModel> createSettingsModels(final RedshiftLoaderNodeSettings cs) {
        return asList(cs.getTargetFolderModel(), cs.getTableNameModel(), cs.getFileFormatSelectionModel(),
            cs.getAuthorizationModel(), cs.getFileFormatModel(), cs.getCompressionModel(), cs.getChunkSizeModel(),
            cs.getFileSizeModel());
    }

    @Override
    public DBTable load(final ExecutionParameters<RedshiftLoaderNodeSettings> parameters) throws Exception {
        final RedshiftLoaderNodeSettings customSettings = parameters.getCustomSettings();
        final DBTable table = customSettings.getTableNameModel().toDBTable();
        final RedshiftLoaderFileFormat fileFormat =
            RedshiftLoaderFileFormat.optionalValueOf(customSettings.getFileFormatSelectionModel().getStringValue())
                .orElseThrow(() -> new InvalidSettingsException("No file format is selected."));
        final DBPortObject dbPortObject = parameters.getDBPortObject();
        final ExecutionMonitor exec = parameters.getExecutionMonitor();
        validateColumns(false, exec, parameters.getRowInput().getDataTableSpec(), dbPortObject, table);
        exec.setMessage("Columns validated");
        exec.setProgress(0.1);

        //write file
        final DBSession session = dbPortObject.getDBSession();
        try (final DBFileWriter<RedshiftLoaderNodeSettings, RedshiftLoaderSettings> writer = fileFormat.getWriter()) {
            final FSPath targetFile = writer.write(exec, parameters);
            //load file into database table
            final SettingsModelWriterFileChooser targetFolderModel = customSettings.getTargetFolderModel();
            try (FSConnection connection = targetFolderModel.getConnection()) {
                final URIExporter uriExporter = connection.getDefaultURIExporterFactory().getExporter();
                final String targetFileString = URIUtil.toUnencodedString(uriExporter.toUri(targetFile));
                LOGGER.debugWithFormat("Target file: \"%s\"", targetFileString);
                exec.setProgress("Loading data file into DB table...");
                exec.checkCanceled();
                session.getAgent(DBLoader.class).load(exec,
                    new DBLoadTableFromFileParameters<>(DBLoaderMode.REMOTE_TEMPORARY_FILE, targetFileString,
                    table, writer.getLoadParameter(customSettings)));
            }
        }
        // Output
        return table;
    }

    @Override
    public void loadDialogSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs,
        final List<DialogComponent> dialogComponents, final RedshiftLoaderNodeComponents customComponents)
        throws NotConfigurableException {
        m_init = true;
        super.loadDialogSettingsFrom(settings, specs, dialogComponents, customComponents);
        onFileFormatSelectionChange(customComponents);
        m_init = false;
    }
}

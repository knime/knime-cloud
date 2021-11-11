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
 *   10 Nov 2021 (Tobias): created
 */
package org.knime.cloud.aws.redshift.connector2.loader.writer.redshift;

import static org.knime.database.util.CsvFiles.writeCsv;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.knime.base.node.io.csvwriter.FileWriterSettings;
import org.knime.cloud.aws.redshift.connector2.loader.RedshiftLoaderFileFormat;
import org.knime.cloud.aws.redshift.connector2.loader.RedshiftLoaderNodeSettings;
import org.knime.cloud.aws.redshift.connector2.loader.RedshiftLoaderSettings;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.database.node.io.load.ExecutionParameters;
import org.knime.database.node.io.load.impl.fs.DBFileLoadUtil;
import org.knime.database.node.io.load.impl.fs.DBFileLoadUtil.DBFileLoader;
import org.knime.database.node.io.load.impl.fs.util.DBSingleFileWriter;
import org.knime.filehandling.core.connections.FSPath;

/**
 * {@link DBFileLoader} implementation that writes out a csv file.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("deprecation")
public class RedshiftCsvWriter extends DBSingleFileWriter<RedshiftLoaderNodeSettings, RedshiftLoaderSettings> {

    private static final String FILE_EXTENSION = ".csv";

    @Override
    public RedshiftLoaderSettings getLoadParameter(final RedshiftLoaderNodeSettings cs) {
        final RedshiftLoaderFileFormat fileFormat =
            RedshiftLoaderFileFormat.optionalValueOf(cs.getFileFormatSelectionModel().getStringValue())
                .orElseThrow(() -> new IllegalStateException("No file format is selected."));
        return new RedshiftLoaderSettings(fileFormat, cs.getAuthorizationModel().getStringValue(),
            cs.getFileFormatModel().getFileWriterSettings(), cs.getCompressionModel().getStringValue(),
            cs.getChunkSizeModel().getIntValue(), cs.getFileSizeModel().getLongValue());
    }

    @Override
    protected FSPath writeData(final ExecutionMonitor exec,
        final ExecutionParameters<RedshiftLoaderNodeSettings> parameters, final FSPath rootFolder)
        throws IOException, CanceledExecutionException {

        final RedshiftLoaderNodeSettings customSettings = parameters.getCustomSettings();
        final FileWriterSettings fileWriterSettings = customSettings.getFileFormatModel().getFileWriterSettings();

        final boolean useGzip =
                !RedshiftLoaderFileFormat.NONE_COMPRESSION.equals(customSettings.getCompressionModel().getStringValue());
        final String fileExtension;
        if (useGzip) {
            fileExtension = FILE_EXTENSION + ".gz";
        } else {
            fileExtension = FILE_EXTENSION;
        }
        final FSPath tempFile = createTempFile(rootFolder, fileExtension);
        try (OutputStream outputStream = DBFileLoadUtil.createOutputStream(tempFile);
                OutputStream os = useGzip ? new GZIPOutputStream(outputStream) : outputStream) {
            writeCsv(parameters.getRowInput(), os, fileWriterSettings, exec);
            exec.setMessage("Uploading file (no progress available)...");
        }
        return tempFile;
    }
}
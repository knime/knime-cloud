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
 *   Nov 18, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.google.cloud.storage.filehandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import com.google.api.client.http.FileContent;
import com.google.api.services.storage.Storage;

/**
 * Wrapper around a temporary file that gets uploaded on close.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class UploadOutputStream extends FileOutputStream {
    private final File m_tmpFile;
    private final Storage m_client;
    private final String m_bucketName;
    private final String m_objectName;

    /**
     * Default constructor.
     *
     * @param client client to use for upload
     * @param bucketName bucket to use
     * @param objectName name of new object
     * @param tmpFile temporary file
     */
    UploadOutputStream(final Storage client, final String bucketName, final String objectName, final File tmpFile) throws FileNotFoundException {
        super(tmpFile);
        m_client = client;
        m_bucketName = bucketName;
        m_objectName = objectName;
        m_tmpFile = tmpFile;
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            m_client.objects()
                .insert(m_bucketName, null, new FileContent(null, m_tmpFile))
                .setName(m_objectName)
                .execute();
        } catch (final Exception e) {
            throw new IOException(e);
        } finally {
            Files.delete(m_tmpFile.toPath());
        }
    }
}

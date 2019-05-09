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
 *   Apr 8, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.nodes.rekognition.faces;

import java.nio.ByteBuffer;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.image.png.PNGImageBlobCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.BufferedDataTableRowOutput;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.Attribute;
import com.amazonaws.services.rekognition.model.DetectFacesRequest;
import com.amazonaws.services.rekognition.model.DetectFacesResult;
import com.amazonaws.services.rekognition.model.FaceDetail;
import com.amazonaws.services.rekognition.model.Image;



/**
 *
 * @author jfalgout
 */
public class FacesOperation {
    private final ConnectionInformation m_cxnInfo;
    private final String m_imageColumnName;

    FacesOperation(final ConnectionInformation cxnInfo, final String imageColumnName) {
        this.m_cxnInfo = cxnInfo;
        this.m_imageColumnName = imageColumnName;
    }

    BufferedDataTable compute(final ExecutionContext exec, final BufferedDataTable data) throws CanceledExecutionException, InterruptedException, InvalidSettingsException {
        final BufferedDataContainer dc = exec.createDataContainer(FacesOperation.createDataTableSpec(m_imageColumnName));
        if (data.size() == 0) {
            dc.close();
            return dc.getTable();
        }

        // Create stream enabled input and output ports wrapping the input data table and output table.
        DataTableRowInput in = new DataTableRowInput(data);
        BufferedDataTableRowOutput out = new BufferedDataTableRowOutput(dc);

        // Invoke computation on the stream enabled ports
        try {
            compute(in, out, exec, in.getRowCount());
        }
        finally {
            in.close();
            out.close();
        }

        return out.getDataTable();
    }

    void compute(final RowInput in, final RowOutput out, final ExecutionContext exec, final long rowCount) throws CanceledExecutionException, InterruptedException {

        // Build credentials and provider using account info from the connection object
        // User -> access key ID, password -> secret key
        BasicAWSCredentials creds = new BasicAWSCredentials(m_cxnInfo.getUser(), m_cxnInfo.getPassword());
        AWSCredentialsProvider credProvider = new AWSStaticCredentialsProvider(creds);

        // Create a connection to the Translate service in the provided region

        AmazonRekognition rekog = AmazonRekognitionClientBuilder.standard()
                .withCredentials(credProvider)
                .withRegion(m_cxnInfo.getHost())
                .build();

        int imgColumnIdx = in.getDataTableSpec().findColumnIndex(m_imageColumnName);
        long inputRowIndex = 0;
        long rowCounter = 0;

        DataRow inputRow = null;
        while ((inputRow = in.poll()) != null) {

            PNGImageBlobCell imageCell = (PNGImageBlobCell) inputRow.getCell(imgColumnIdx);
            byte[] imageBytes = imageCell.getImageContent().getByteArray();
            ByteBuffer imgBuffer = ByteBuffer.wrap(imageBytes);

            Image img = new Image().withBytes(imgBuffer);

            DetectFacesRequest request =
                    new DetectFacesRequest()
                        .withImage(img)
                        .withAttributes(Attribute.ALL);

            DetectFacesResult result = rekog.detectFaces(request);
            int faceCount = result.getFaceDetails().size();
            System.out.println("# faces = " + faceCount);

            long outputRowIndex = 0;
            for (FaceDetail faceDetails : result.getFaceDetails()) {
                // Make row key unique with the input row number and the sequence number of each token
                RowKey key = new RowKey("Row " + inputRowIndex + "_" + outputRowIndex++);

                // Create cells containing the output data
                DataCell[] cells = new DataCell[5];
                cells[0] = imageCell.getImageContent().toImageCell();
                cells[1] = new StringCell(faceDetails.getGender().getValue());
                cells[2] = new DoubleCell(faceDetails.getGender().getConfidence());
                cells[3] = new IntCell(faceDetails.getAgeRange().getLow());
                cells[4] = new IntCell(faceDetails.getAgeRange().getHigh());


                // Create a new data row and push it to the output container.
                DataRow outputRow = new DefaultRow(key, cells);
                out.push(outputRow);
            }

            ++inputRowIndex;
        }

    }

    static DataTableSpec createDataTableSpec(final String imageColumnName) {
        // Repeat the input text column adding in a column for the translated text.
        // TODO copy all input columns to the output
        DataColumnSpec[] allColSpecs = new DataColumnSpec[5];
        allColSpecs[0] = new DataColumnSpecCreator(imageColumnName, DataType.getType(PNGImageBlobCell.class)).createSpec();
        allColSpecs[1] = new DataColumnSpecCreator("Gender", StringCell.TYPE).createSpec();
        allColSpecs[2] = new DataColumnSpecCreator("Gender Confidene", DoubleCell.TYPE).createSpec();
        allColSpecs[3] = new DataColumnSpecCreator("Lower Age", IntCell.TYPE).createSpec();
        allColSpecs[4] = new DataColumnSpecCreator("Upper Age", IntCell.TYPE).createSpec();

        return new DataTableSpec(allColSpecs);
    }

}

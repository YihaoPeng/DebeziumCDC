package sharebase.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource.Context;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

public class DecredFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(Context context, DataType producedDataType) {
        // create type information for the DeserializationSchema
        final TypeInformation<RowData> producedTypeInfo = (TypeInformation<RowData>) context.createTypeInformation(producedDataType);

        // most of the code in DeserializationSchema will not work on internal data
        // structures
        // create a converter for conversion at the end
        final DataStructureConverter converter = context.createDataStructureConverter(producedDataType);

        // use logical types during runtime for parsing
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

        // create runtime class
        return new DecredFormatSchema(parsingTypes, converter, producedTypeInfo);
    }

}

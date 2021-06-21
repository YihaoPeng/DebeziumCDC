package sharebase.format;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.connector.RuntimeConverter.Context;

import sharebase.Decred;
public class DecredFormatSchema implements DeserializationSchema<RowData> {

    private final List<LogicalType> parsingTypes;
    private final DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;

    public DecredFormatSchema(List<LogicalType> parsingTypes, DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public void open(InitializationContext context) {
        // converters must be opened
        converter.open(Context.create(DecredFormatSchema.class.getClassLoader()));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        Decred.DecredMsg share = Decred.DecredMsg.parseFrom(message);
        return (RowData) converter.toInternal(share);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

}

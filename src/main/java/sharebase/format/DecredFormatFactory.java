package sharebase.format;

import java.util.Set;
import java.util.Collections;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;

public class DecredFormatFactory implements DeserializationFormatFactory {

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(Context context, ReadableConfig options) {
        // create and return the format
        return new DecredFormat();
    }

    @Override
    public String factoryIdentifier() {
        return "sharelog-dcr";
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }
    
}

// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.example;

import com.google.gson.Gson;
import com.risingwave.functions.DataTypeHint;
import com.risingwave.functions.PeriodDuration;
import com.risingwave.functions.ScalarFunction;
import com.risingwave.functions.TableFunction;
import com.risingwave.functions.UdfServer;
import com.example.gen.RecordKey;
import com.example.gen.RecordValue;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List; 

public class ProtoDecodeUdf {
    public static void main(String[] args) throws IOException {
        try (var server = new UdfServer("0.0.0.0", 8815)) {
            server.addFunction("proto_header_decode", new ProtoHeaderDecode());
            server.addFunction("proto_decode_snapshot", new ProtoSnapShotDecode());
            server.addFunction("proto_decode_trade", new ProtoTradeDecode());
            server.addFunction("proto_decode_elixir_instrument", new ProtoElixirInstrumentDecode());
            server.start();
            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static class ProtoHeaderDecode implements ScalarFunction {
        public static class ProtoHeader{
            public String stream;
            public String pan;
        }
        public ProtoHeader eval(byte[] binaryData) {
            try {
              var protoData = RecordKey.DataKey.parseFrom(binaryData);
              ProtoHeader header = new ProtoHeader();
              header.stream = protoData.getStream();
              header.pan = protoData.getPan();
              return header;
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
              return null;
            }
        }
    }
    // {"account_snapshot", "trade", "elixir_instrument"}
    public static class ProtoSnapShotDecode implements ScalarFunction {
        public static class accountSnapshot {
            public Long ds;
            public String instance;
            public String marketType;
            public Long eventTime;
            public Long logTime;
            public Double totalValue;
            public Double cash;
            public Double frozenCash;
            public Double dailyPnl;
            public Double marketValue;
            public Double margin;
            public Double commission;
            public Double serverMargin;
            public Double serverCommission;
        }
        public accountSnapshot eval(byte[] data) {
            try {
              var snapshotData = RecordValue.AccountSnapshot.parseFrom(data);
              var accountSnapshot = new accountSnapshot();
              accountSnapshot.ds = snapshotData.getDs();
              accountSnapshot.instance = snapshotData.getInstance();
              accountSnapshot.marketType = snapshotData.getMarketType();
              accountSnapshot.eventTime = snapshotData.getEventTime();
              accountSnapshot.logTime = snapshotData.getLogTime();
              accountSnapshot.totalValue = snapshotData.getTotalValue();
              accountSnapshot.cash = snapshotData.getCash();
              accountSnapshot.frozenCash = snapshotData.getFrozenCash();
              accountSnapshot.dailyPnl = snapshotData.getDailyPnl();
              accountSnapshot.marketValue = snapshotData.getMarketValue();
              accountSnapshot.margin = snapshotData.getMargin();
              accountSnapshot.commission = snapshotData.getCommission();
              accountSnapshot.serverMargin = snapshotData.getServerMargin();
              accountSnapshot.serverCommission = snapshotData.getServerCommission();
              return accountSnapshot;
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
              return null;
            }
        }
    }
    public static class ProtoTradeDecode implements ScalarFunction {
        public static class trade {
            public Long ds;
            public String instance;
            public String marketType;
            public Long eventTime;
            public Long logTime;

            public String orderRef;
            public String code;
            public String direction;
            public String offset;
            public Double value;
            public Double price;
            public Double volume;
        }
        public trade eval(byte[] data) {
            try {
              var trade_proto = RecordValue.Trade.parseFrom(data);
              var tradedata = new trade();
              tradedata.ds = trade_proto.getDs();
              tradedata.instance = trade_proto.getInstance();
              tradedata.marketType = trade_proto.getMarketType();
              tradedata.eventTime = trade_proto.getEventTime();
              tradedata.orderRef = trade_proto.getOrderRef();
              tradedata.code = trade_proto.getCode();
              tradedata.direction = trade_proto.getDirection();
              tradedata.offset = trade_proto.getOffset();
              tradedata.value = trade_proto.getValue();
              tradedata.price = trade_proto.getPrice();
              tradedata.volume = trade_proto.getVolume();
              return tradedata;
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
              return null;
            }
        }
    }
    public static class ProtoElixirInstrumentDecode implements ScalarFunction {
        static Gson gson = new Gson();

        public static class Row {
          public String code;
          public Double[] data = new Double[0];
        }
        public static class elixirInstrument {
          public Long ds;
          public String instance;
          public String marketType;
          public Long eventTime;
          public Long logTime;
          public String[] columns = new String[0];
          public String[] rows = new String[0];
        }
        public elixirInstrument eval(byte[] data) {
            try {
              var elixirInstrument = RecordValue.ElixirInstrument.parseFrom(data);
              var elixirInstrumentData = new elixirInstrument();
              elixirInstrumentData.ds = elixirInstrument.getDs();
              elixirInstrumentData.instance = elixirInstrument.getInstance();
              elixirInstrumentData.marketType = elixirInstrument.getMarketType();
              elixirInstrumentData.eventTime = elixirInstrument.getEventTime();
              elixirInstrumentData.logTime = elixirInstrument.getLogTime();
              elixirInstrumentData.columns = elixirInstrument.getColumnsList().toArray(new String[0]);
              int count = elixirInstrument.getRowsCount();
              System.out.println("value" + count);
              elixirInstrumentData.rows = new String[count];
              // elixirInstrumentData.rows[0] = "1";
              for (int i = 0; i < count; ++i) {
                var row = new Row(); 
                row.code = elixirInstrument.getRows(i).getCode();
                row.data = elixirInstrument.getRows(i).getDataList().toArray(new Double[0]);
                var str = gson.toJson(row);
                // System.out.println("i" + i + "length"+ elixirInstrumentData.rows.length);
                elixirInstrumentData.rows[i] = str;
              }
              return elixirInstrumentData;
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
              return null;
            }
        }
    }

    public static class ProtoDecodeRow implements ScalarFunction {
        static Gson gson = new Gson();

        public static class Row {
          public String code;
          public Double[] data = new Double[0];
        }
        public Row eval(@DataTypeHint("JSONB") String data) {
          if(data == null)
            return null;
          Row row = gson.fromJson(data, Row.class);
          return row;
        }
    }
}
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.DataInputStream;

public class VehicleTelematics {

    /** The logger used by the environment and its subclasses. */
    //protected static final Logger LOG = (Logger) LoggerFactory.getLogger(VehicleTelematics.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 2){
            System.err.println("Error: Wrong Input! Please provide correct inputs");
            return;
        }

        String inputFile = args[0];
        String outputFile = args[1];

        final ExecutionEnvironment env2= ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Vehicle> vehicleObj = env2.readCsvFile(inputFile).pojoType(Vehicle.class,
                "eventTime", "vehicleID", "speed", "xway", "lane", "direction", "segment", "position");;


        //speedfine -radar 1st one
        DataSet<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedFineVehicle = vehicleObj.filter(new FilterFunction<Vehicle>(){
            @Override
            public boolean filter(Vehicle item) throws Exception {
                if (item.getSpeed() > 90) {
                    return true;
                } else {
                    return false;
                }

            }

        }).map(new MapFunction<Vehicle, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Vehicle vehicle) throws Exception {
                return (new Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>(
                        vehicle.getTimeStamp(),
                        vehicle.getID(),
                        vehicle.getXway(),
                        vehicle.getSegment(),
                        vehicle.getDirection(),
                        vehicle.getSpeed()));
            }
        });

        speedFineVehicle.writeAsCsv(outputFile + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE);
        //speedFineVehicle.writeAsCsv("speedfine.csv", FileSystem.WriteMode.OVERWRITE);



        //Average Speed Fine
        DataSet<Tuple6<Integer,Integer, Integer, Integer, Integer, Float>> averageSpeedFines = vehicleObj.filter(new FilterFunction<Vehicle>(){
            @Override
            public boolean filter(Vehicle item) throws Exception {
                if (item.getSegment()>=52 && item.getSegment()<=56) {
                    return true;
                } else {
                    return false;
                }

            }

        }).flatMap(new FlatMapFunction<Vehicle, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>() {
            @Override
            public void flatMap(Vehicle vehicles, Collector<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> collector) throws Exception {

                Tuple6<Integer,Integer, Integer, Integer, Integer, Integer> t = new Tuple6<>(vehicles.getTimeStamp(),vehicles.getID(),vehicles.getSpeed(),vehicles.getXway(),vehicles.getSegment(),vehicles.getDirection());
                collector.collect(t);
            }
        }).groupBy(1).sortGroup(0, Order.ASCENDING).reduceGroup(new GroupReduceFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>, Tuple6<Integer,Integer,Integer,Integer,Integer,Float>>() {
            @Override
            public void reduce(Iterable<Tuple6<Integer,Integer, Integer, Integer, Integer, Integer>> iterableVehicle,Collector<Tuple6<Integer,Integer, Integer, Integer, Integer, Float>> out) throws Exception {

                int averageSpeed = 0;
                boolean flag = false;
                int count=0;
                int time1=0;
                int time2=0;



               for(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> item : iterableVehicle){



                    if((item.f4.equals(52) && item.f5.equals(0) && !flag) || (item.f4.equals(56) && item.f5.equals(1) && !flag)) {
                        //averageSpeed = item.f2;
                        //if(input.iterator().next().f4.equals(53))
                        time1=item.f0;
                        flag = true;


                    }
                    if(flag){
                        averageSpeed=averageSpeed+item.f2;
                        count++;

                        if((item.f4.equals(52) && item.f5.equals(1)) || (item.f4.equals(56) && item.f5.equals(0))) {
                            flag = false;
                            time2=item.f0;

                            //System.out.println("son"+item);
                            float avg = (float) averageSpeed/count;
                            if((avg)>60){
                                out.collect(new Tuple6<Integer,Integer,Integer,Integer,Integer,Float>(time1,time2,item.f1,item.f3,item.f5,avg));
                                //System.out.println("vehicle: "+item.f1+"time1: "+time1+"time2: "+time2+"average speed: "+averageSpeed/count);
                            }
                            averageSpeed=0;
                            count=0;
                            time1=0;
                            time2=0;
                            break;
                        }

                    }

                }

            }
        });

        averageSpeedFines.writeAsCsv(outputFile +"/avgspeedfines.csv",FileSystem.WriteMode.OVERWRITE);


        //Accidents
        //as we use window function, we need stream execution
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> lines = env.readTextFile(inputFile);

        SingleOutputStreamOperator<Vehicle> vehicles = lines.map(new MapFunction<String, Vehicle>() {
            @Override
            public Vehicle map(String input) throws Exception {

                String[] split = input.split(",");
                int time = Integer.parseInt(split[0]);
                int vID = Integer.parseInt(split[1]);
                int speed = Integer.parseInt(split[2]);
                int xway = Integer.parseInt(split[3]);
                int lane = Integer.parseInt(split[4]);
                int dir = Integer.parseInt(split[5]);
                int seg = Integer.parseInt(split[6]);
                int pos = Integer.parseInt(split[7]);
                return new Vehicle(time,vID, speed,xway,lane,dir,seg,pos);
            }
        });



        SingleOutputStreamOperator<Tuple6<Integer,Integer, Integer, Integer, Integer, Integer>> stoppedVehicles = vehicles.filter(new FilterFunction<Vehicle>(){
            @Override
            public boolean filter(Vehicle item) throws Exception {
                if (item.getSpeed()==0) {
                    return true;
                } else {
                    return false;
                }

            }

        }).flatMap(new FlatMapFunction<Vehicle, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>() {
            @Override
            public void flatMap(Vehicle vehicles, Collector<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> collector) throws Exception {

                Tuple6<Integer,Integer, Integer, Integer, Integer, Integer> t = new Tuple6<>(vehicles.getTimeStamp(),vehicles.getID(),vehicles.getXway(),vehicles.getSegment(),vehicles.getDirection(),vehicles.getPosition());
                collector.collect(t);
            }
        });

       KeyedStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,Tuple> ky= stoppedVehicles.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Integer,Integer, Integer, Integer, Integer, Integer>>(){
            @Override
            public long extractAscendingTimestamp(Tuple6<Integer,Integer, Integer, Integer, Integer, Integer> element) {
                return element.f0*1000;
            }

        }).keyBy(1);

        SingleOutputStreamOperator<Tuple7<Integer,Integer, Integer, Integer, Integer, Integer, Integer>> accidents = ky.window(SlidingEventTimeWindows.of(Time.seconds(120),Time.seconds(30),Time.seconds(90))).apply(new WindowFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple, TimeWindow>()
        { @Override
        public void apply(Tuple key, TimeWindow window, Iterable<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> input,
                          Collector<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> out) throws Exception {
            int averageSpeed = 0;
            boolean flag = false;
            int count=0;
            int time1=0;
            int time2=0;
            int old_pos=0;
            //System.out.println(window);
            for(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> item : input){

                if(count==0 && flag==false) {

                    time1 = item.f0;
                    old_pos=item.f5;
                    flag = true;
                    count++;
                    //System.out.println("hey" +item +"    item" + count);
                }
                else if( old_pos==item.f5 && count<4 && flag==true){
                    count++;
                    //System.out.println("helloooo" + count);
                }
                if(count==4 && flag==true)
                {
                    time2=item.f0;
                    flag=false;
                    count=0;
                    old_pos=0;
                    out.collect(new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(time1,time2,item.f1,item.f2,item.f3,item.f4,item.f5));
                    time1=0;
                    time2=0;
                }
            }

        }

        });

        //stoppedVehicles.writeAsCsv(outputFile + "/accidents.csv", FileSystem.WriteMode.OVERWRITE);
        accidents.writeAsCsv(outputFile+ "/accidents.csv", FileSystem.WriteMode.OVERWRITE);


        try {
            env.execute();
            env2.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }



}





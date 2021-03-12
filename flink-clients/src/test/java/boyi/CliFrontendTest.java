package boyi;

import org.apache.flink.client.cli.CliFrontend;

public class CliFrontendTest {

    public static void main(String[] args) {

        String [] param = new String[8] ;
        param[0] = "run" ;
        param[1] = "-t" ;
        param[2] = "yarn-per-job" ;
        param[3] = "-c" ;
        param[4] = "org.apache.flink.streaming.examples.socket.SocketWindowWordCount" ;
        param[5] = "/opt/tools/flink-1.12.2/examples/streaming/SocketWindowWordCount.jar" ;
        param[6] = "--port" ;
        param[7] = "9999" ;

        CliFrontend.main(param);

    }

}

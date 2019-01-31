package test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.stream.Collectors;

public class EventsEmitterServerJava extends Thread {
    @Override
    public void run() {
        System.out.println("Events emitter server thread started");

        InputStream stream = getClass().getResourceAsStream("/events.jsonl");
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        List<String> lines = reader.lines().collect(Collectors.toList());

        try {
            ServerSocket server = new ServerSocket(9999);

            while (true) {
                Socket s = server.accept();
                PrintStream out = new PrintStream(s.getOutputStream());

                lines.forEach(line -> {
                    out.println(line.trim());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    out.flush();
                });

                s.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

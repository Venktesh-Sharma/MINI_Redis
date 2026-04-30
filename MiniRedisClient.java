package com.miniredis.cli;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

/**
 * Interactive CLI client for Mini-Redis.
 *
 * Connects to the server over TCP and relays commands / responses.
 * Supports a background reader thread to display async PubSub messages.
 *
 * Usage:
 *   java -cp miniredis.jar com.miniredis.cli.MiniRedisClient [host] [port]
 */
public class MiniRedisClient {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int    DEFAULT_PORT = 6399;

    // ANSI color codes for prettier output
    private static final String RESET  = "\u001B[0m";
    private static final String GREEN  = "\u001B[32m";
    private static final String RED    = "\u001B[31m";
    private static final String YELLOW = "\u001B[33m";
    private static final String CYAN   = "\u001B[36m";
    private static final String BOLD   = "\u001B[1m";

    public static void main(String[] args) throws IOException {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int    port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;

        System.out.println(BOLD + CYAN + "Mini-Redis CLI" + RESET);
        System.out.println("Connecting to " + host + ":" + port + " ...");

        try (Socket socket = new Socket(host, port);
             BufferedReader serverIn = new BufferedReader(
                 new InputStreamReader(socket.getInputStream()));
             PrintWriter serverOut = new PrintWriter(
                 new OutputStreamWriter(socket.getOutputStream()), true);
             Scanner userIn = new Scanner(System.in)) {

            System.out.println(GREEN + "Connected!" + RESET + " Type HELP for commands.\n");

            // Background thread to receive server push messages (PubSub, async)
            Thread reader = new Thread(() -> {
                try {
                    String line;
                    while ((line = serverIn.readLine()) != null) {
                        printResponse(line);
                    }
                } catch (IOException e) {
                    System.out.println(RED + "[Disconnected from server]" + RESET);
                }
            }, "server-reader");
            reader.setDaemon(true);
            reader.start();

            // Main loop: read user input, send to server
            while (reader.isAlive()) {
                System.out.print(BOLD + "mini-redis> " + RESET);

                if (!userIn.hasNextLine()) break;
                String cmd = userIn.nextLine().trim();
                if (cmd.isEmpty()) continue;

                serverOut.println(cmd);

                // Short sleep to let background reader print the response
                Thread.sleep(80);

                if (cmd.equalsIgnoreCase("QUIT") || cmd.equalsIgnoreCase("EXIT")) break;
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println(RED + "Error: " + e.getMessage() + RESET);
        }

        System.out.println(YELLOW + "Goodbye!" + RESET);
    }

    private static void printResponse(String response) {
        if (response == null) return;
        if (response.startsWith("+")) {
            System.out.println(GREEN + response.substring(1) + RESET);
        } else if (response.startsWith("-ERR") || response.startsWith("-")) {
            System.out.println(RED + response + RESET);
        } else if (response.startsWith(":")) {
            System.out.println(CYAN + "(integer) " + response.substring(1) + RESET);
        } else if (response.startsWith("$nil")) {
            System.out.println(YELLOW + "(nil)" + RESET);
        } else if (response.startsWith("$")) {
            System.out.println("\"" + response.substring(1) + "\"");
        } else if (response.startsWith("*MESSAGE")) {
            System.out.println(BOLD + YELLOW + response + RESET);
        } else {
            System.out.println(response);
        }
    }
}

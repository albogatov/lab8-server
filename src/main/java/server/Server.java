package server;

import commons.app.*;
import commons.commands.Login;
import commons.commands.Register;
import commons.commands.Save;
import commons.elements.Worker;
import commons.network.Request;
import commons.network.Response;
import commons.network.ResponseCode;
import commons.network.ResponseData;
import commons.utils.InteractionInterface;
import commons.utils.Storage;
import server.interaction.StorageInteraction;
import commons.utils.UserInterface;
import commons.utils.ConnectionSource;
import commons.utils.DataBaseCenter;
import commons.utils.SerializationTool;

import java.io.*;
import java.net.*;
import java.nio.channels.DatagramChannel;
import java.sql.Connection;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server implements Runnable, ConnectionSource {
    public static final Logger logger = Logger.getLogger(
            Server.class.getName());
    private final DataBaseCenter dataBaseCenter;
    private String[] arguments;
    private DatagramSocket datagramSocket;
    private final UserInterface userInterface = new UserInterface(new InputStreamReader(System.in),
            new OutputStreamWriter(System.out), true);
    private final int port = 7855;
    private final Storage storage = new Storage();
    private InteractionInterface interactiveStorage = null;
    private final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(10);
    private boolean updateNeed = false;

    public static void main(String[] args) {
        logger.log(Level.INFO, "commons.app.server operation initiated");
        Server server = new Server(new DataBaseCenter());
        server.setArguments(args);
        server.run();
    }

    public Server(DataBaseCenter dbc) {
        this.dataBaseCenter = dbc;
    }

    public void setArguments(String[] arguments) {
        logger.log(Level.INFO, "Setting server's arguments");
        this.arguments = arguments;
    }

    public Request receive() throws SocketTimeoutException {
        logger.log(Level.INFO, "Receiving initiated");
        byte[] receiver = new byte[1000000];
        DatagramPacket inCommand = new DatagramPacket(receiver, receiver.length);
        Request request;
        try {
            logger.log(Level.INFO, "Receiving command from client");
            datagramSocket.receive(inCommand);
            request = (Request) new SerializationTool().deserializeObject(receiver);
            InetAddress clientAddress = inCommand.getAddress();
            CommandCenter.setClientAddress(clientAddress);
            int clientPort = inCommand.getPort();
            CommandCenter.setClientPort(clientPort);
//            System.out.println("received request");
            return request;
        } catch (IOException e) {
            logger.log(Level.SEVERE, "An I/O Exception has occurred", e);
            if (e instanceof SocketTimeoutException)
                throw new SocketTimeoutException("Timeout!!!");
            return null;
        }
    }

    public boolean processRequest(Request request) {
        try {
            String argument;
            Worker worker;
            Command cmd;
            if (!(request.getCommandName().equals("login") || request.getCommandName().equals("register")))
                cmd = CommandCenter.getInstance().getCmd(request.getCommandName());
            else if (request.getCommandName().equals("login")) {
                cmd = new Login();
            } else cmd = new Register();
            cmd.setUser(request.getUser());
            cmd.setObject((Worker) request.getCommandObjectArgument());
            cmd.setArgument(request.getCommandStringArgument());
            Response response = new Response();
            boolean authorisation = false;
            if (cmd.getClass().toString().contains(".Register")) {
                authorisation = authoriseUser(cmd.getUser(), "new");
                if (!authorisation) {
                    response.setResponseCode(ResponseCode.ERROR);
                } else response.setResponseCode(ResponseCode.OK);
                response.setResponseBody(ResponseData.getAndClear());
                response.setResponseBodyArgs(ResponseData.getArgsAndClear());
                response.setWorkers(interactiveStorage.getStorage().getCollection());
                DatagramPacket responseSender = new DatagramPacket(SerializationTool.serializeObject(response),
                        SerializationTool.serializeObject(response).length, CommandCenter.getClientAddress(), CommandCenter.getClientPort());
                datagramSocket.send(responseSender);
                updateNeed = true;
            }
            if (cmd.getClass().toString().contains(".Login")) {
                authorisation = authoriseUser(cmd.getUser(), "old");
                if (!authorisation) {
                    response.setResponseCode(ResponseCode.ERROR);
                } else response.setResponseCode(ResponseCode.OK);
                response.setResponseBody(ResponseData.getAndClear());
                response.setResponseBodyArgs(ResponseData.getArgsAndClear());
                response.setWorkers(interactiveStorage.getStorage().getCollection());
                DatagramPacket responseSender = new DatagramPacket(SerializationTool.serializeObject(response),
                        SerializationTool.serializeObject(response).length, CommandCenter.getClientAddress(), CommandCenter.getClientPort());
                datagramSocket.send(responseSender);
                updateNeed = true;
            } else {
                authorisation = true;
            }
            if (authorisation && !cmd.getClass().toString().contains(".Login") && !cmd.getClass().toString().contains(".Register")) {
                if (request.getCommandName().equals("show")) {
                    logger.log(Level.INFO, "Update requested");
//                    Command save = new Save();
//                    save.setUser(cmd.getUser());
//                    CommandCenter.getInstance().executeCommand(userInterface, save, interactiveStorage);
                    if (updateNeed) {
                        try {
                            logger.log(Level.INFO, "Executing command without arguments");
                            if (CommandCenter.getInstance().executeCommand(userInterface, cmd, interactiveStorage, dataBaseCenter))
                                response.setResponseCode(ResponseCode.OK);
                            else response.setResponseCode(ResponseCode.ERROR);
                            response.setResponseBody(ResponseData.getAndClear());
                            response.setResponseBodyArgs(ResponseData.getArgsAndClear());
                            response.setWorkers(interactiveStorage.getStorage().getCollection());
                            DatagramPacket datagramPacket = new DatagramPacket(SerializationTool.serializeObject(response),
                                    SerializationTool.serializeObject(response).length, CommandCenter.getClientAddress(), CommandCenter.getClientPort());
                            Thread.sleep(300);
                            datagramSocket.send(datagramPacket);
                            updateNeed = false;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        try {
                            response.setResponseBody("NotNeeded");
                            response.setResponseBodyArgs(null);
                            response.setWorkers(interactiveStorage.getStorage().getCollection());
                            DatagramPacket datagramPacket = new DatagramPacket(SerializationTool.serializeObject(response),
                                    SerializationTool.serializeObject(response).length, CommandCenter.getClientAddress(), CommandCenter.getClientPort());
                            Thread.sleep(300);
                            datagramSocket.send(datagramPacket);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    try {
                        updateNeed = true;
                        if (cmd.getArgumentAmount() == 0) {
                            logger.log(Level.INFO, "Executing command without arguments");
                            if (request.getCommandName().equals("clear"))
                                updateNeed = true;
                            if (CommandCenter.getInstance().executeCommand(userInterface, cmd, interactiveStorage, dataBaseCenter))
                                response.setResponseCode(ResponseCode.OK);
                            else response.setResponseCode(ResponseCode.ERROR);
                        }
                        if (cmd.getArgumentAmount() == 1 && !cmd.getNeedsObject()) {
                            if (request.getCommandName().equals("remove_by_id"))
                                updateNeed = true;
                            logger.log(Level.INFO, "Executing command with a String argument");
                            argument = cmd.getArgument();
                            if (CommandCenter.getInstance().executeCommand(userInterface, cmd, argument, interactiveStorage, dataBaseCenter))
                                response.setResponseCode(ResponseCode.OK);
                            else response.setResponseCode(ResponseCode.ERROR);
                        }
                        if (cmd.getArgumentAmount() == 1 && cmd.getNeedsObject()) {
                            updateNeed = true;
                            logger.log(Level.INFO, "Executing command with an object as an argument");
                            worker = cmd.getObject();
                            if (CommandCenter.getInstance().executeCommand(userInterface, cmd, interactiveStorage, worker, dataBaseCenter))
                                response.setResponseCode(ResponseCode.OK);
                            else response.setResponseCode(ResponseCode.ERROR);
                        }
                        if (cmd.getArgumentAmount() == 2 && cmd.getNeedsObject()) {
                            updateNeed = true;
                            logger.log(Level.INFO, "Executing command with arguments of various types");
                            argument = cmd.getArgument();
                            worker = cmd.getObject();
                            if (CommandCenter.getInstance().executeCommand(userInterface, cmd, argument, interactiveStorage, worker, dataBaseCenter))
                                response.setResponseCode(ResponseCode.OK);
                            else response.setResponseCode(ResponseCode.ERROR);
                        }
                        response.setResponseBody(ResponseData.getAndClear());
                        response.setResponseBodyArgs(ResponseData.getArgsAndClear());
                        response.setWorkers(interactiveStorage.getStorage().getCollection());
                        DatagramPacket datagramPacket = new DatagramPacket(SerializationTool.serializeObject(response),
                                SerializationTool.serializeObject(response).length, CommandCenter.getClientAddress(), CommandCenter.getClientPort());
                        Thread.sleep(300);
                        datagramSocket.send(datagramPacket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void run() {
        logger.log(Level.INFO, "The server is now operational");
        interactiveStorage = new StorageInteraction(storage);
        try {
            try {
                logger.log(Level.INFO, "Reading the collection from database");
                dataBaseCenter.setPassword(arguments[0]);
                dataBaseCenter.createTable();
                dataBaseCenter.retrieveCollectionFromDB(interactiveStorage);
            } catch (NullPointerException e) {
                logger.log(Level.SEVERE, "Data is invalid", e);
                System.exit(-1);
            } catch (DateTimeParseException e) {
                logger.log(Level.SEVERE, "Date formatting is invalid", e);
                System.exit(-1);
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.log(Level.SEVERE, "Not enough arguments", e);
                System.exit(-1);
            } catch (IllegalArgumentException e) {
                logger.log(Level.SEVERE, "Invalid arguments", e);
                System.exit(-1);
            }
            datagramSocket = new DatagramSocket(port);
            userInterface.connectToServer(datagramSocket);
            logger.log(Level.INFO, "Collection successfully uploaded");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.log(Level.INFO, "Collection saving...");
                CommandCenter.getInstance().executeServerCommand(new Save(), interactiveStorage);
            }));
            while (true) {
                try {
                    datagramSocket.setSoTimeout(600 * 1000);
                    Request request = fixedThreadPool.submit(this::receive).get();
                    boolean success = fixedThreadPool.submit(() -> processRequest(request)).get();
                } catch (IOException e) {
                    if (e instanceof SocketTimeoutException) {
                        logger.log(Level.SEVERE, "Timeout is reached", e);
                    } else {
                        logger.log(Level.SEVERE, "Unexpected issue occured", e);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.log(Level.SEVERE, "An Exception has occurred", e);
        } finally {
            try {
                logger.log(Level.INFO, "Collection saving...");
                CommandCenter.getInstance().executeServerCommand(new Save(), interactiveStorage);
                logger.log(Level.INFO, "server shutting down");
                System.exit(0);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "An unknown Exception has occurred", e);
                System.exit(-1);
            }
        }
    }

    public boolean authoriseUser(User user, String existence) {
        if (existence.equals("new")) {
            if (dataBaseCenter.addUser(user)) {
                CommandCenter.getInstance().executeCommand(userInterface, new Register(), true);
                return true;
            } else {
                CommandCenter.getInstance().executeCommand(userInterface, new Register(), false);
                return false;
            }
        } else {
            if (dataBaseCenter.loginUser(user)) {
                CommandCenter.getInstance().executeCommand(userInterface, new Login(), true);
                return true;
            } else {
                CommandCenter.getInstance().executeCommand(userInterface, new Login(), false);
                return false;
            }
        }
    }
}

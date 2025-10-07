package one.utilix;

import java.io.*;
import java.sql.*;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.*;

public class App {

    private static final Logger logger = Logger.getLogger(App.class.getName());

    public static void main(String[] args) {
//        String path_data = "./";
//        String path_archive = "archive/";
//        String path_processed = "processed/";
        String path_data = "/home/forengelman/ftp/files";
        String path_archive = "/home/forengelman/ftp/files/archive/";
        String path_processed = "/home/forengelman/ftp/files/processed/";

        setupLogger();

        ensureDirectoriesExist(path_data, path_archive, path_processed);

        File folder = new File(path_data);
//        System.out.println(folder.getAbsolutePath());
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".csv"));
//        System.out.println(files.length);
        if (files == null || files.length == 0) {
            logger.info("No CSV files found in the Data folder.");
            return;
        }

        String dbUrl = "jdbc:mysql://localhost:3306/mbus?autoReconnect=true&useSSL=false&serverTimezone=UTC";
        String dbUser = "mysqlengelman";
        String dbPassword = "LkzTyutkmvfyf2025!";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                logger.info("Connected to the database.");

                for (File file : files) {
                    logger.info("Found file: " + file.getName());
                    //  Перевірка стабільності файлу
                    if (!isFileStable(file, 3, 2000)) {
                        logger.warning("File " + file.getName() + " is still being uploaded. Skipping...");
                        continue; // пропускаємо і чекаємо наступний цикл
                    }
                    logger.info("Processing file: " + file.getName());
                    try {
                        List<String[]> processedData = processCsvFile(file, connection);
                        File archiveFile = new File(path_archive + file.getName());
                        moveFile(file, archiveFile);
                        File processedFile = new File(path_processed + file.getName());
                        writeToCsv(processedData, processedFile);
                    } catch (Exception e) {
                        logger.severe("Error processing file " + file.getName() + ": " + e.getMessage());
                    }
                }
            } catch (SQLException e) {
                logger.severe("Database connection error: " + e.getMessage());
            }
        } catch (ClassNotFoundException e) {
            logger.severe("Driver not found! " + e.getMessage());
        }
    }


    private static void ensureDirectoriesExist(String... directories) {
        for (String dir : directories) {
            File folder = new File(dir);
            if (!folder.exists()) {
                if (folder.mkdirs()) {
                    logger.info("Created directory: " + dir);
                } else {
                    logger.severe("Failed to create directory: " + dir);
                }
            } else {
                //logger.info("Directory exists: " + dir);
            }
        }
    }

    // Зчитування CSV з додатковими перевірками
    private static List<String[]> processCsvFile(File file, Connection connection) throws IOException {
        List<String[]> processedData = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                if (!Objects.equals(values[0], "SND_NR")) {
                    //logger.warning("Skipping malformed line in file " + file.getName() + ": " + line);
                    continue;
                }
//                System.out.println(line);
//                for(int i=0; i< values.length; i++) {
                    //System.out.println(i + "  " + values[i]);
//                }
                String nzav = values[6];
                String string_value = values[32];
                String string_time = values[31];
                //String string_status = values[11];
                String string_error = values[15];
                String string_type = values[9];
                float pokaz;
                //long unixtime;
                Timestamp unixtime;
                int status_flag;
                String meter_type = "";

//                System.out.println("nzav: "+ nzav);

                // get value
                try {
                    pokaz = Float.parseFloat(string_value.replace(',', '.'));
                } catch (NumberFormatException e) {
                    logger.warning("Invalid pokaz value in file " + file.getName() + ": " + string_value);
                    continue;
                }
//                System.out.println("pokaz: "+ pokaz);

                // get time
                try {
                    String clean = string_time.split(" ")[0] + " " + string_time.split(" ")[1];
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");
                    LocalDateTime ldt = LocalDateTime.parse(clean, formatter);
                    //unixtime = ldt.atZone(ZoneId.of("UTC")).toEpochSecond();
                    unixtime = Timestamp.valueOf(ldt);
                } catch (NumberFormatException e) {
                    logger.warning("Invalid timestamp in file " + file.getName() + ": " + string_time);
                    continue;
                }
//                System.out.println("unixtime: "+ unixtime);

                // get meter type
                try {
                    if ("ColdWater".equalsIgnoreCase(string_type)) {
                        meter_type = "CWM";
                    } else
                    if ("WWater".equalsIgnoreCase(string_type)) {
                        meter_type = "HWM";
                    } else
                    if ("Heat".equalsIgnoreCase(string_type)) {
                        meter_type = "HM";
                    }
                } catch (NumberFormatException e) {
                    logger.warning("Invalid meter type value in file " + file.getName() + ": " + string_type);
                    continue;
                }
//                System.out.println("meter_type: "+ meter_type);
//                System.out.println("string_type: "+ string_type);

                // get errors
                try {
                    if (string_error.endsWith("b") || string_error.endsWith("B")) {
                        string_error = string_error.substring(0, string_error.length() - 1);
                    }
                    status_flag = Integer.parseInt(string_error, 2);
                } catch (NumberFormatException e) {
                    logger.warning("Invalid error value in file " + file.getName() + ": " + string_error);
                    continue;
                }
//                System.out.println("status_flag: "+ status_flag);
//                System.out.println("string_error: "+ string_error);


                // get kvk_id
                Integer kvkId = getKvkId(connection, nzav);
                if (kvkId == null) {
                    logger.warning("No kvk_id found for nzav: " + nzav);
                    continue;
                }

                saveToDatabase(connection, kvkId, pokaz, unixtime, nzav, meter_type, status_flag);
                processedData.add(new String[]{String.valueOf(kvkId), nzav, String.valueOf(pokaz), String.valueOf(unixtime)});
            }
        } catch (IOException e) {
            logger.severe("Error reading file " + file.getName() + ": " + e.getMessage());
            throw e;
        }

        return processedData;
    }


    // Метод перевірки розміру
    private static boolean isFileStable(File file, int attempts, int delayMs) {
        long previousLength = -1;
        for (int i = 0; i < attempts; i++) {
            long length = file.length();
            if (length == previousLength) {
                return true; // розмір не змінився — файл стабільний
            }
            previousLength = length;
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    // Метод для отримання kvk_id за nzav
    private static Integer getKvkId(Connection connection, String nzav) {
        String query = "SELECT kvk_id FROM network WHERE meter_number = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, nzav);
//            System.out.println(nzav);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("kvk_id");
            } else {
                return null;
            }
        } catch (SQLException e) {
            logger.severe("Error querying kvk_id: " + e.getMessage());
            return null;
        }
    }

    private static void saveToDatabase(Connection connection, int kvkId, float pokaz, Timestamp unixtime, String nzav, String meter_type, int status) {
        String query = "INSERT INTO meters_values (kvk_id, unixtime, pokaz, nzav, sent, meter_type) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, kvkId);
            preparedStatement.setTimestamp(2, unixtime);
            preparedStatement.setFloat(3, pokaz);
            preparedStatement.setString(4, nzav);
            preparedStatement.setInt(5, 0);
            preparedStatement.setString(6, meter_type);
            preparedStatement.executeUpdate();
            logger.info("Data saved for kvk_id: " + kvkId);
        } catch (SQLException e) {
            logger.severe("Error inserting data into table meters_values: " + e.getMessage());
        }
        // save errors
        if (status != 0 & status > 4) { //inserting raw errors to DB 0-4 default value
            query = "INSERT INTO meters_errors (kvk_id, unixtime, err_no, nzav, sent, meter_type) VALUES (?,?,?,?,?,?);";
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setInt(1, kvkId);
                preparedStatement.setTimestamp(2, unixtime);
                preparedStatement.setFloat(3, status);
                preparedStatement.setString(4, nzav);
                preparedStatement.setInt(5, 0);
                preparedStatement.setString(6, meter_type);
                preparedStatement.executeUpdate();
                logger.info("Data saved for kvk_id: " + kvkId);
            } catch (SQLException e) {
                logger.severe("Error inserting data into table meters_errors: " + e.getMessage());
            }
        }
        if ((status & (1 << 4)) != 0) {//Magnet
            query = "INSERT INTO meters_errors (kvk_id, unixtime, err_no, nzav, sent, meter_type) VALUES (?,?,?,?,?,?);";
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setInt(1, kvkId);
                preparedStatement.setTimestamp(2, unixtime);
                preparedStatement.setFloat(3, 1); // magnet manipulation
                preparedStatement.setString(4, nzav);
                preparedStatement.setInt(5, 0);
                preparedStatement.setString(6, meter_type);
                preparedStatement.executeUpdate();
                logger.info("Data saved for kvk_id: " + kvkId);
            } catch (SQLException e) {
                logger.severe("Error inserting data into table meters_errors: " + e.getMessage());
            }
        }
        if ((status & (1 << 3)) != 0) {//Influence
            query = "INSERT INTO meters_errors (kvk_id, unixtime, err_no, nzav, sent, meter_type) VALUES (?,?,?,?,?,?);";
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setInt(1, kvkId);
                preparedStatement.setTimestamp(2, unixtime);
                preparedStatement.setFloat(3, 2); // influence
                preparedStatement.setString(4, nzav);
                preparedStatement.setInt(5, 0);
                preparedStatement.setString(6, meter_type);
                preparedStatement.executeUpdate();
                logger.info("Data saved for kvk_id: " + kvkId);
            } catch (SQLException e) {
                logger.severe("Error inserting data into table meters_errors: " + e.getMessage());
            }
        }
        if ((status & (1 << 7)) != 0) { // backward flow
            query = "INSERT INTO meters_errors (kvk_id, unixtime, err_no, nzav, sent, meter_type) VALUES (?,?,?,?,?,?);";
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setInt(1, kvkId);
                preparedStatement.setTimestamp(2, unixtime);
                preparedStatement.setFloat(3, 3); // backward flow
                preparedStatement.setString(4, nzav);
                preparedStatement.setInt(5, 0);
                preparedStatement.setString(6, meter_type);
                preparedStatement.executeUpdate();
                logger.info("Data saved for kvk_id: " + kvkId);
            } catch (SQLException e) {
                logger.severe("Error inserting data into table meters_errors: " + e.getMessage());
            }
        }
        if (((status & (1)) != 0) | ((status & (1 << 2)) != 0)) {//Damage
            query = "INSERT INTO meters_errors (kvk_id, unixtime, err_no, nzav, sent, meter_type) VALUES (?,?,?,?,?,?);";
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setInt(1, kvkId);
                preparedStatement.setTimestamp(2, unixtime);
                preparedStatement.setFloat(3, 4); // Damage
                preparedStatement.setString(4, nzav);
                preparedStatement.setInt(5, 0);
                preparedStatement.setString(6, meter_type);
                preparedStatement.executeUpdate();
                logger.info("Data saved for kvk_id: " + kvkId);
            } catch (SQLException e) {
                logger.severe("Error inserting data into table meters_errors: " + e.getMessage());
            }
        }
    }


    private static void moveFile(File source, File destination) {
//        System.out.println("Moving from " + source.getAbsolutePath() + " to " + destination.getAbsolutePath());
        if (source.renameTo(destination)) {
            logger.info("File moved to: " + destination.getAbsolutePath());
        } else {
            logger.severe("Failed to move file: " + source.getName());
        }
    }

    private static void writeToCsv(List<String[]> data, File file) {
//        file = new File("Test/Processed/aaa.csv");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            for (String[] row : data) {
                bw.write(String.join(",", row));
                bw.newLine();
            }
            logger.info("Processed data saved to: " + file.getAbsolutePath());
        } catch (IOException e) {
            logger.severe("Error writing to file " + file.getName() + ": " + e.getMessage());
        }
    }

    private static void setupLogger() {
        try {
            FileHandler fileHandler = new FileHandler("Parser.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setLevel(Level.ALL);
            logger.info("Logger initialized.");
        } catch (IOException e) {
            System.err.println("Failed to initialize logger: " + e.getMessage());
        }
    }
}

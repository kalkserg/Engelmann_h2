package one.utilix;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.logging.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

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

        File[] files = null;           // масив CSV файлів
        if (args.length > 0) {
            switch (args[0]) {
                case "-h":
                case "--help":
                    printUsage();
                    return;
                case "-f":
                    if (args.length < 2) {
                        System.out.println("Error: Missing file path after -f");
                        return;
                    }
                    String filePath = args[1];
                    File singleFile = new File(filePath);
                    if (!singleFile.exists()) {
                        System.out.println("Error: File not found -> " + filePath);
                        return;
                    }
                    files = new File[]{ singleFile }; // переопределюємо files
                    break;
                default:
                    System.out.println("Unknown option: " + args[0]);
                    printUsage();
                    return;
            }
        }

        // === Якщо files ще не визначено, читаємо всю папку ===
        if (files == null) {
            File folder = new File(path_data);
            files = folder.listFiles((dir, name) -> name.endsWith(".csv"));
        }

        if (files == null || files.length == 0) {
            logger.info("No CSV files found in the Data folder.");
            return;
        }

//        File folder = new File(path_data);
////        System.out.println(folder.getAbsolutePath());
//        File[] files = folder.listFiles((dir, name) -> name.endsWith(".csv"));
////        System.out.println(files.length);
//        if (files == null || files.length == 0) {
//            logger.info("No CSV files found in the Data folder.");
//            return;
//        }

        String dbUrl = "jdbc:mysql://localhost:3306/mbus?autoReconnect=true&useSSL=false&serverTimezone=UTC";
        String dbUser = "mysqlengelman";
        String dbPassword = "LkzTyutkmvfyf2025!";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                logger.info("Connected to the database.");


                for (File file : files) {
                    logger.info("Found file: " + file.getName());

                    // Перевіряємо, що це CSV
                    if (!file.getName().endsWith(".csv")) {
                        logger.info("Skipping non-CSV file: " + file.getName());
                        continue;
                    }

                    // Визначаємо відповідний лог-файл
                    String logFileName = file.getName().replace(".csv", ".log");
                    File logFile = new File(file.getParent(), logFileName);

                    boolean success = true; // за замовчуванням обробляємо CSV
                    if (logFile.exists()) {
                        // Перевіряємо лог
                        success = false;
                        try {
                            List<String> lines = Files.readAllLines(logFile.toPath());
                            for (String line : lines) {
                                if (line.trim().equals("195=0")) {
                                    success = true;
                                    break;
                                }
                            }
                        } catch (IOException e) {
                            logger.warning("Cannot read log file: " + logFile.getName());
                        }

                        if (!success) {
                            logger.warning("Log file indicates CSV is not ready: " + logFile.getName() + ". Skipping...");
                            continue;
                        }
                    } else {
                        logger.info("Log file not found for " + file.getName() + ". Processing CSV anyway.");
                    }

                    // Перевірка стабільності файлу
                    if (!isFileStable(file, 3, 2000)) {
                        logger.warning("File " + file.getName() + " is still being uploaded. Skipping...");
                        continue;
                    }

                    // Обробка CSV
                    logger.info("Processing file: " + file.getName());
                    try {
                        List<String[]> processedData = processCsvFile(file, connection);
                        File archiveFile = new File(path_archive + file.getName());
                        moveFile(file, archiveFile);
                        File processedFile = new File(path_processed + file.getName());
                        writeToCsv(processedData, processedFile);

                        // Якщо лог існує — видаляємо його
                        if (logFile.exists() && logFile.delete()) {
                            logger.info("Deleted log file: " + logFile.getName());
                        } else if (logFile.exists()) {
                            logger.warning("Failed to delete log file: " + logFile.getName());
                        }

                    } catch (Exception e) {
                        logger.severe("Error processing file " + file.getName() + ": " + e.getMessage());
                    }
                }


                /// ///////////////////
//                for (File file : files) {
//                    logger.info("Found file: " + file.getName());
//                    //  Перевірка стабільності файлу
//                    if (!isFileStable(file, 3, 2000)) {
//                        logger.warning("File " + file.getName() + " is still being uploaded. Skipping...");
//                        continue; // пропускаємо і чекаємо наступний цикл
//                    }
//                    logger.info("Processing file: " + file.getName());
//                    try {
//                        List<String[]> processedData = processCsvFile(file, connection);
//                        File archiveFile = new File(path_archive + file.getName());
//                        moveFile(file, archiveFile);
//                        File processedFile = new File(path_processed + file.getName());
//                        writeToCsv(processedData, processedFile);
//                    } catch (Exception e) {
//                        logger.severe("Error processing file " + file.getName() + ": " + e.getMessage());
//                    }
//                }

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

    // Зчитування CSV
    private static List<String[]> processCsvFile(File file, Connection connection) throws IOException {
        List<String[]> processedData = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {

            String headerLine = null;
            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();

                // Пропуск пустих рядків
                if (line.isEmpty()) continue;

                // Пропуск сигнальних рядків
                if (line.equals("«File completely written»")) continue;

                // Якщо це новий заголовок
                if (line.startsWith("Frame Type")) {
                    headerLine = line;
                    continue;
                }

                // Якщо є заголовок — обробляємо дані
                if (headerLine != null) {
                    String[] headers = headerLine.split(";");
                    String[] values = line.split(";");
                    Map<String, String> record = new HashMap<>();

                    for (int i = 0; i < Math.min(headers.length, values.length); i++) {
                        record.put(headers[i].trim(), values[i].trim());
                    }

                    try {
                        String nzav = record.containsKey("Meter ID") ? record.get("Meter ID").trim() : "";
                        String stringValue = record.containsKey("IV,0,0,0,m^3,Vol") ? record.get("IV,0,0,0,m^3,Vol").trim() : "";
                        String stringTime = record.containsKey("IV,0,0,0,,Date/Time") ? record.get("IV,0,0,0,,Date/Time").trim() : "";
                        String stringError = record.containsKey("IV,0,0,0,,ErrorFlags(binary)(deviceType specific)") ?
                                record.get("IV,0,0,0,,ErrorFlags(binary)(deviceType specific)").trim() : "";
                        String stringType = record.containsKey("Device Type") ? record.get("Device Type").trim() : "";

                        if (nzav.isEmpty() || stringValue.isEmpty() || stringTime.isEmpty()) {
                            logger.warning("Line " + lineNumber + ": missing required fields");
                            continue;
                        }

                        float pokaz;
                        Timestamp unixtime;
                        int statusFlag;
                        String meterType;

                        // Парс показників
                        try {
                            pokaz = Float.parseFloat(stringValue.replace(',', '.'));
                        } catch (NumberFormatException e) {
                            logger.warning("Line " + lineNumber + ": invalid pokaz " + stringValue);
                            continue;
                        }

                        // Парс дати
                        try {
                            String[] parts = stringTime.split(" ");
                            String clean = parts[0] + " " + parts[1];
                            SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm");
                            Date date = sdf.parse(clean);
                            unixtime = new Timestamp(date.getTime());
                        } catch (Exception e) {
                            logger.warning("Line " + lineNumber + ": invalid timestamp " + stringTime);
                            continue;
                        }

                        // Тип лічильника (без switch expression)
                        meterType = "UNKNOWN";
                        if (stringType.equalsIgnoreCase("coldwater")) {
                            meterType = "CWM";
                        } else if (stringType.equalsIgnoreCase("wwater")) {
                            meterType = "HWM";
                        } else if (stringType.equalsIgnoreCase("heatminlet")) {
                            meterType = "HM";
                        }

                        // Парс статусу
                        try {
                            if (stringError.endsWith("b") || stringError.endsWith("B")) {
                                stringError = stringError.substring(0, stringError.length() - 1);
                            }
                            statusFlag = Integer.parseInt(stringError, 2);
                        } catch (NumberFormatException e) {
                            logger.warning("Line " + lineNumber + ": invalid error value " + stringError);
                            continue;
                        }

                        // kvk_id
                        Integer kvkId = getKvkId(connection, nzav);
                        if (kvkId == null) {
                            logger.warning("Line " + lineNumber + ": no kvk_id found for nzav " + nzav);
                            continue;
                        }

                        // Зберегти у базу
                        saveToDatabase(connection, kvkId, pokaz, unixtime, nzav, meterType, statusFlag);
                        processedData.add(new String[]{String.valueOf(kvkId), nzav, String.valueOf(pokaz), String.valueOf(unixtime)});

                    } catch (Exception e) {
                        logger.warning("Error processing record at line " + lineNumber + ": " + e.getMessage());
                    }
                }
            }
        }

        return processedData;
    }



//    // Зчитування CSV з додатковими перевірками
//    private static List<String[]> processCsvFile(File file, Connection connection) throws IOException {
//        List<String[]> processedData = new ArrayList<>();
//
//        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
//            String line;
//
//            while ((line = br.readLine()) != null) {
//                String[] values = line.split(";");
//                if (!Objects.equals(values[0], "SND_NR")) {
//                    //logger.warning("Skipping malformed line in file " + file.getName() + ": " + line);
//                    continue;
//                }
////                System.out.println(line);
////                for(int i=0; i< values.length; i++) {
//                    //System.out.println(i + "  " + values[i]);
////                }
//                String nzav = values[6];
//                String string_value = values[32];
//                String string_time = values[31];
//                //String string_status = values[11];
//                String string_error = values[15];
//                String string_type = values[9];
//                float pokaz;
//                //long unixtime;
//                Timestamp unixtime;
//                int status_flag;
//                String meter_type = "";
//
////                System.out.println("nzav: "+ nzav);
//
//                // get value
//                try {
//                    pokaz = Float.parseFloat(string_value.replace(',', '.'));
//                } catch (NumberFormatException e) {
//                    logger.warning("Invalid pokaz value in file " + file.getName() + ": " + string_value);
//                    continue;
//                }
////                System.out.println("pokaz: "+ pokaz);
//
//                // get time
//                try {
//                    String clean = string_time.split(" ")[0] + " " + string_time.split(" ")[1];
//                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");
//                    LocalDateTime ldt = LocalDateTime.parse(clean, formatter);
//                    //unixtime = ldt.atZone(ZoneId.of("UTC")).toEpochSecond();
//                    unixtime = Timestamp.valueOf(ldt);
//                } catch (NumberFormatException e) {
//                    logger.warning("Invalid timestamp in file " + file.getName() + ": " + string_time);
//                    continue;
//                }
////                System.out.println("unixtime: "+ unixtime);
//
//                // get meter type
//                try {
//                    if ("ColdWater".equalsIgnoreCase(string_type)) {
//                        meter_type = "CWM";
//                    } else
//                    if ("WWater".equalsIgnoreCase(string_type)) {
//                        meter_type = "HWM";
//                    } else
//                    if ("Heat".equalsIgnoreCase(string_type)) {
//                        meter_type = "HM";
//                    }
//                } catch (NumberFormatException e) {
//                    logger.warning("Invalid meter type value in file " + file.getName() + ": " + string_type);
//                    continue;
//                }
////                System.out.println("meter_type: "+ meter_type);
////                System.out.println("string_type: "+ string_type);
//
//                // get errors
//                try {
//                    if (string_error.endsWith("b") || string_error.endsWith("B")) {
//                        string_error = string_error.substring(0, string_error.length() - 1);
//                    }
//                    status_flag = Integer.parseInt(string_error, 2);
//                } catch (NumberFormatException e) {
//                    logger.warning("Invalid error value in file " + file.getName() + ": " + string_error);
//                    continue;
//                }

    /// /                System.out.println("status_flag: "+ status_flag);
    /// /                System.out.println("string_error: "+ string_error);
//
//
//                // get kvk_id
//                Integer kvkId = getKvkId(connection, nzav);
//                if (kvkId == null) {
//                    logger.warning("No kvk_id found for nzav: " + nzav);
//                    continue;
//                }
//
//                saveToDatabase(connection, kvkId, pokaz, unixtime, nzav, meter_type, status_flag);
//                processedData.add(new String[]{String.valueOf(kvkId), nzav, String.valueOf(pokaz), String.valueOf(unixtime)});
//            }
//        } catch (IOException e) {
//            logger.severe("Error reading file " + file.getName() + ": " + e.getMessage());
//            throw e;
//        }
//
//        return processedData;
//    }


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

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar Parser.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -h, --help       Show this help message");
        System.out.println("  -f <file>        Specify CSV file to process");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar Parser.jar -h");
        System.out.println("  java -jar Parser.jar -f data.csv");
    }
}

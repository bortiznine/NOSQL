
import com.datastax.driver.core.*;
import jnr.ffi.annotations.In;
import org.apache.cassandra.serializers.TimestampSerializer;

import java.util.Scanner;


public class Main {

    public static void main(String[] args) throws InterruptedException {

        int input=3;
        while (input!=0) {
            System.out.println("Welcome to The Medical DB");
            System.out.println("Are you a entering a New Patient(1)\nLooking up Previous Patient(2)\nUpdate Existing Patient Info(3)\nExit (0)");
            Scanner sc = new Scanner(System.in);
            System.out.println("Enter a value :");
            input = sc.nextInt();
            System.out.println("User input: " + input);
            switch (input) {
                case 1 -> {
                    try {


                        //inputs for data to insert
                        System.out.println("Let's insert data for the new patient");
                        System.out.println("Please enter name:");
                        Scanner sc2 = new Scanner(System.in);
                        String name = sc2.nextLine();
                        System.out.println("User input: " + name);

                        System.out.println("Please enter address:");
                        Scanner sc3 = new Scanner(System.in);
                        String address = sc3.nextLine();
                        System.out.println("User input: " + address);

                        System.out.println("Please enter insurance company:");
                        Scanner sc4 = new Scanner(System.in);
                        String insurance = sc4.nextLine();
                        System.out.println("User input: " + insurance);

                        System.out.println("Please enter Profile ID:");
                        Scanner sc5 = new Scanner(System.in);
                        int pid = sc5.nextInt();
                        System.out.println("User input: " + pid);

                        System.out.println("Please enter  Date created following pattern YYYY-MM-DD:");
                        Scanner sc6 = new Scanner(System.in);
                        String date_created = sc6.nextLine();
                        System.out.println("User input: " + date_created);

                        Thread.sleep(2000);
                        System.out.println("We will now enter the Physical Activity for the new Patient:");
                        Thread.sleep(2000);

                        System.out.println("Please enter the name of the activity completed :");
                        Scanner sc7 = new Scanner(System.in);
                        String activity = sc7.nextLine();
                        System.out.println("User input: " + activity);

                        System.out.println("Please enter how long the activity was in minutes:");
                        Scanner sc8 = new Scanner(System.in);
                        Double activity_duration = sc8.nextDouble();
                        System.out.println("User input: " + activity_duration);

                        Thread.sleep(2000);
                        System.out.println("We will now enter the new Patient's Vitals:");
                        Thread.sleep(2000);

                        System.out.println("Please enter the patient's blood pressure (SYS/DIAS):");
                        Scanner sc9 = new Scanner(System.in);
                        String blood_pressure = sc9.nextLine();
                        System.out.println("User input: " + blood_pressure);

                        System.out.println("Please enter the patient's heart rate:");
                        Scanner sc10 = new Scanner(System.in);
                        Integer heart_rate = sc10.nextInt();
                        System.out.println("User input: " + heart_rate);

                        System.out.println("Please enter the patient's Sp02(Oxygen Levels):");
                        Scanner sc11 = new Scanner(System.in);
                        Integer sp02 = sc11.nextInt();
                        System.out.println("User input: " + sp02);


                        System.out.println("Cassandra Java Connection");
                        Cluster cluster;
                        Session session;
                        //Connect to the cluster and Keyspace ecommerce
                        cluster = Cluster.builder().addContactPoint("localhost").build();
                        session = (Session) cluster.connect("medical_db");

                        PreparedStatement result = session.prepare("INSERT INTO patient_personal_info (pid, address,date_created,insurance, name) VALUES (?,?,?,?,?);");
                        BoundStatement boundStatement = new BoundStatement(result);
                        ResultSet results = session.execute(boundStatement.bind(
                                pid, address, date_created, insurance, name));
                        System.out.println("Insert Successful\n");

                        PreparedStatement result2 = session.prepare("select * from patient_personal_info where name=? ALLOW FILTERING");
                        BoundStatement boundStatement2 = new BoundStatement(result2);
                        ResultSet resultset = session.execute(boundStatement2.bind(name));
                        for (Row row : resultset) {
                            pid = row.getInt("pid");
                            address = row.getString("address");
                            insurance = row.getString("insurance");
                            name = row.getString("name");


                            System.out.println("Patient ID: " + pid);
                            System.out.println("Name: " + name);
                            System.out.println("Address: " + address);
                            System.out.println("Insurance: " + insurance);
                            System.out.println("Dated Created: " + date_created);
                        }

                        PreparedStatement result_activity = session.prepare("INSERT INTO patient_activity (pid, duration,last_activity, name,time) VALUES (?,?,?,?,?);");
                        BoundStatement boundStatement_activity = new BoundStatement(result_activity);
                        ResultSet results_activity = session.execute(boundStatement_activity.bind(
                                pid, activity_duration, activity, name, date_created));

                        PreparedStatement result4 = session.prepare("select * from patient_activity where name=? ALLOW FILTERING;");
                        BoundStatement boundStatement4 = new BoundStatement(result4);
                        ResultSet resultset_activity = session.execute(boundStatement4.bind(name));
                        for (Row row_activity : resultset_activity) {
                            pid = row_activity.getInt("pid");
                            activity_duration = row_activity.getDouble("duration");
                            activity = row_activity.getString("last_activity");
                            name = row_activity.getString("name");


                            System.out.println("Duration: " + activity_duration);
                            System.out.println("Last Activity: " + activity);
                            System.out.println("Time Stamp: " + date_created);


                        }

                        PreparedStatement result_vitals = session.prepare("INSERT INTO patient_vitals (pid, blood_pressure,heartrate, name,sp02) VALUES (?,?,?,?,?);");
                        BoundStatement boundStatement_vitals = new BoundStatement(result_vitals);
                        ResultSet results_vitals = session.execute(boundStatement_vitals.bind(
                                pid, blood_pressure, heart_rate, name, sp02));

                        PreparedStatement result_vitals2 = session.prepare("select * from patient_vitals where name=? ALLOW FILTERING;");
                        BoundStatement boundStatement_vitals2 = new BoundStatement(result_vitals2);
                        ResultSet resultset_vitals2 = session.execute(boundStatement_vitals2.bind(name));
                        cluster.close();
                        for (Row row_vitals : resultset_vitals2) {

                            blood_pressure = row_vitals.getString("blood_pressure");
                            heart_rate = row_vitals.getInt("heartrate");
                            name = row_vitals.getString("name");
                            sp02 = row_vitals.getInt("sp02");


                            System.out.println("Patient ID: " + pid);
                            System.out.println("Blood Pressure: " + blood_pressure);
                            System.out.println("Heart Rate: " + heart_rate);
                            System.out.println("Oxygen Levels: " + sp02);


                        }


                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    Thread.sleep(5000);
                }
                case 2 -> {
                    try {
                        System.out.println("Please Select a Query to run to find data:\n\n");
                        System.out.println("Find all patients by name with heart rates higher than 90 BPM (1) \n");
                        System.out.println("Find all patients by Patient ID and name that did cardio last activity and duration was 90+ minutes (2) \n");
                        System.out.println("Find the pid,name,blood_pressure of all patients by name that have SP02 lower than 95% (3) \n");
                        System.out.println("Find the date patients were added and Insurance Company of patients with the name 'Victor James' (4) \n");
                        System.out.println("See all patient personal information (5) \n");
                        System.out.println("See all patient activities (6) \n");
                        System.out.println("See all patient vitals (7) \n");
                        System.out.println("Return to Main Menu (0) \n");


                        Scanner caseTwo_sc1 = new Scanner(System.in);
                        Integer input2 = caseTwo_sc1.nextInt();
                        System.out.println("User input: " + input2);
                        switch (input2) {
                            case 1:

                                Cluster cluster;
                                Session session;
                                //Connect to the cluster and Keyspace ecommerce
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                ResultSet resultSet = session.execute("select name from patient_vitals where heartrate>=90 ALLOW FILTERING;");
                                cluster.close();
                                System.out.println("Results\n");
                                for (Row row_q1 : resultSet) {
                                    System.out.println(row_q1.getString("name"));

                                }
                                break;
                            case 2:

                                //Connect to the cluster and Keyspace ecommerce
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                ResultSet resultSet2 = session.execute("select pid,name from patient_activity where last_activity ='cardio' and duration>=90 ALLOW FILTERING;");
                                cluster.close();
                                System.out.println("Results\n");
                                for (Row row_q2 : resultSet2) {
                                    System.out.println(row_q2.getInt("pid") + "|" + row_q2.getString("name"));
                                }

                                break;
                            case 3:

                                //Connect to the cluster and Keyspace ecommerce
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                ResultSet resultSet3 = session.execute("select pid,name,blood_pressure from patient_vitals where sp02<95 ALLOW FILTERING;");
                                cluster.close();
                                System.out.println("Results\n");
                                for (Row row_q3 : resultSet3) {
                                    System.out.println(row_q3.getInt("pid") + "|" + row_q3.getString("name") + "|" + row_q3.getString("blood_pressure"));
                                }
                                break;
                            case 4:

                                //Connect to the cluster and Keyspace ecommerce
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                ResultSet resultSet4 = session.execute("select date_created,insurance from patient_personal_info where name='Victor James'  ALLOW FILTERING;");
                                cluster.close();
                                System.out.println("Results\n");
                                for (Row row_q4 : resultSet4) {
                                    System.out.println(row_q4.getString("date_created") + "|" + row_q4.getString("insurance"));
                                }
                                break;

                            case 5:

                                //Connect to the cluster and Keyspace ecommerce
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                ResultSet resultSet5 = session.execute("select * from patient_personal_info ALLOW FILTERING;");
                                cluster.close();
                                System.out.println("Results\n");
                                for (Row row5 : resultSet5) {
                                   int pid = row5.getInt("pid");
                                   String address = row5.getString("address");
                                  String  insurance = row5.getString("insurance");
                                    String name = row5.getString("name");
                                    String date_created= row5.getString("date_created");


                                    System.out.println("Patient ID: " + pid);
                                    System.out.println("Name: " + name);
                                    System.out.println("Address: " + address);
                                    System.out.println("Insurance: " + insurance);
                                    System.out.println("Dated Created: " + date_created+"\n");
                                }
                                break;

                            case 6:

                                //Connect to the cluster and Keyspace ecommerce
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                ResultSet resultSet6 = session.execute("select * from patient_activity ALLOW FILTERING;");
                                cluster.close();
                                System.out.println("Results\n");
                                for (Row row6 : resultSet6) {
                                    int pid = row6.getInt("pid");
                                    double activity_duration = row6.getDouble("duration");
                                   String activity = row6.getString("last_activity");
                                    String name = row6.getString("name");
                                    String date_created= row6.getString("time");


                                    System.out.println("Duration: " + activity_duration);
                                    System.out.println("Last Activity: " + activity);
                                    System.out.println("Time Stamp: " + date_created);
                                    System.out.println("Patient ID: " + pid);
                                    System.out.println("Name: " + name+ "\n");


                                }
                                break;

                            case 7:

                                //Connect to the cluster and Keyspace ecommerce
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                ResultSet resultSet7 = session.execute("select * from patient_vitals ALLOW FILTERING;");
                                cluster.close();
                                System.out.println("Results\n");
                                for (Row row7 : resultSet7) {

                                    String blood_pressure = row7.getString("blood_pressure");
                                   int heart_rate = row7.getInt("heartrate");
                                   String name = row7.getString("name");
                                    int sp02 = row7.getInt("sp02");
                                    int pid = row7.getInt("pid");


                                    System.out.println("Patient ID: " + pid);
                                    System.out.println("Name: " + name);
                                    System.out.println("Blood Pressure: " + blood_pressure);
                                    System.out.println("Heart Rate: " + heart_rate);
                                    System.out.println("Oxygen Levels: " + sp02+ "\n");


                                }
                                break;
                            case 0:
                                System.out.println("Returning to Main Menu!");
                        }
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    Thread.sleep(5000);
                }
                case 3 -> {
                    try {
                        System.out.println("Update a Patients Records!\n\n");
                        System.out.println("Which Table would you like to update records: \n");
                        System.out.println("Patient's Info (1): \n");
                        System.out.println("Patient's Vitals (2):\n");
                        System.out.println("Patient's Physical activity(3) \n");
                        System.out.println("Return to Main Menu (0) \n");
                        Scanner caseThree_sc1 = new Scanner(System.in);
                        Integer input3 = caseThree_sc1.nextInt();
                        System.out.println("User input: " + input3);
                        switch (input3) {
                            case 1:
                                System.out.println("What is the name/patient ID created of the Patient that needs updating: \n");
                                System.out.println("Name:");
                                Scanner caseThree_sc2 = new Scanner(System.in);
                                String findName = caseThree_sc2.nextLine();
                                System.out.println("Patient ID:");
                                Scanner caseThree_sc3 = new Scanner(System.in);
                                Integer findID = caseThree_sc3.nextInt();

                                Cluster cluster;
                                Session session;
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                PreparedStatement result_nameFind = session.prepare("select pid from patient_personal_info where name=?ALLOW FILTERING;");
                                BoundStatement boundStatement_name = new BoundStatement(result_nameFind);
                                ResultSet resultset_namefind = session.execute(boundStatement_name.bind(findName));
                                cluster.close();

                                for (Row row : resultset_namefind) {
                                    Integer pidFind = row.getInt("pid");
                                    if (findID.equals(pidFind)) {
                                        Thread.sleep(1000);
                                        System.out.println("Data found for Patient!");
                                        Thread.sleep(2000);

                                        System.out.println("Let's Update the record: \n");
                                        System.out.println("Which record are we updating? \n");
                                        System.out.println("Address (1) \n");
                                        System.out.println("Insurance (2) \n");
                                        System.out.println("Cancel (0) \n");
                                        Scanner caseThree_sc4 = new Scanner(System.in);
                                        Integer input4 = caseThree_sc4.nextInt();
                                        if (input4 == 1) {
                                            System.out.println("Please enter address:");
                                            Scanner sc3 = new Scanner(System.in);
                                            String addressUpdate = sc3.nextLine();
                                            System.out.println("User input: " + addressUpdate);
                                            cluster = Cluster.builder().addContactPoint("localhost").build();
                                            session = (Session) cluster.connect("medical_db");
                                            PreparedStatement result_update_info = session.prepare("update patient_personal_info set address=? where pid=?;");
                                            BoundStatement boundStatement_updateAddress = new BoundStatement(result_update_info);
                                            ResultSet resultset_addressUpdate = session.execute(boundStatement_updateAddress.bind(addressUpdate, findID));
                                            cluster.close();
                                            System.out.println("Update for Address Complete!");
                                        } else if (input4 == 2) {
                                            System.out.println("Please enter insurance company:");
                                            Scanner sc4 = new Scanner(System.in);
                                            String insuranceUpdate = sc4.nextLine();
                                            System.out.println("User input: " + insuranceUpdate);
                                            cluster = Cluster.builder().addContactPoint("localhost").build();
                                            session = (Session) cluster.connect("medical_db");
                                            PreparedStatement result_update_info = session.prepare("update patient_personal_info set insurance=? where pid=?;");
                                            BoundStatement boundStatement_updateAddress = new BoundStatement(result_update_info);
                                            ResultSet resultset_addressUpdate = session.execute(boundStatement_updateAddress.bind(insuranceUpdate, findID));
                                            cluster.close();
                                            System.out.println("Update for Insurance Complete!");
                                        } else {
                                            System.out.println("Canceling Update!");
                                        }


                                    } else {
                                        System.out.println("Looks like this patient does not exist. please refer to the Main Menu to add new patient!\n");
                                    }


                                }


                                break;
                            case 2:
                                System.out.println("What is the name/patient ID created of the Patient that needs updating: \n");
                                System.out.println("Name:");
                                Scanner caseThree_sc4 = new Scanner(System.in);
                                String findName2 = caseThree_sc4.nextLine();
                                System.out.println("Patient ID:");
                                Scanner caseThree_sc5 = new Scanner(System.in);
                                Integer findID2 = caseThree_sc5.nextInt();
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                PreparedStatement result_nameFind2 = session.prepare("select pid from patient_vitals where name=? ALLOW FILTERING;");
                                BoundStatement boundStatement_name2 = new BoundStatement(result_nameFind2);
                                ResultSet resultset_namefind2 = session.execute(boundStatement_name2.bind(findName2));
                                for (Row row : resultset_namefind2) {
                                    Integer pidFind = row.getInt("pid");
                                    cluster.close();
                                    if (findID2.equals(pidFind)) {
                                        Thread.sleep(1000);
                                        System.out.println("Data found for Patient!");
                                        Thread.sleep(2000);
                                        System.out.println("Let's Update the record: \n");
                                        System.out.println("Which record are we updating? \n");
                                        System.out.println("Blood Pressure (1) \n");
                                        System.out.println("Heart Rate (2) \n");
                                        System.out.println("SP02 (3) \n");
                                        System.out.println("Cancel (0) \n");
                                        Scanner caseThree_sc6 = new Scanner(System.in);
                                        Integer input5 = caseThree_sc6.nextInt();
                                        if (input5 == 1) {
                                            System.out.println("Please enter blood pressure (SYS/DIAS):");
                                            Scanner sc4 = new Scanner(System.in);
                                            String bpUpdate = sc4.nextLine();
                                            System.out.println("User input: " + bpUpdate);
                                            cluster = Cluster.builder().addContactPoint("localhost").build();
                                            session = (Session) cluster.connect("medical_db");
                                            PreparedStatement result_update_info = session.prepare("update patient_vitals set blood_pressure=? where pid=?;");
                                            BoundStatement boundStatement_updateBP = new BoundStatement(result_update_info);
                                            ResultSet resultset_bpUpdate = session.execute(boundStatement_updateBP.bind(bpUpdate, findID2));
                                            cluster.close();
                                            System.out.println("Update for Blood Pressure Complete!");
                                        } else if (input5 == 2) {
                                            System.out.println("Please enter Heart Rate:");
                                            Scanner sc4 = new Scanner(System.in);
                                            Integer hrUpdate = sc4.nextInt();
                                            System.out.println("User input: " + hrUpdate);
                                            cluster = Cluster.builder().addContactPoint("localhost").build();
                                            session = (Session) cluster.connect("medical_db");
                                            PreparedStatement result_update_info = session.prepare("update patient_vitals set heartrate=? where pid=?;");
                                            BoundStatement boundStatement_hrUpdate = new BoundStatement(result_update_info);
                                            ResultSet resultset_hrUpdate = session.execute(boundStatement_hrUpdate.bind(hrUpdate, findID2));
                                            cluster.close();
                                            System.out.println("Update for Heart Rate Complete!");
                                        } else if (input5 == 3) {
                                            System.out.println("Please enter SP02:");
                                            Scanner sc4 = new Scanner(System.in);
                                            Integer spUpdate = sc4.nextInt();
                                            System.out.println("User input: " + spUpdate);
                                            cluster = Cluster.builder().addContactPoint("localhost").build();
                                            session = (Session) cluster.connect("medical_db");
                                            PreparedStatement result_update_info = session.prepare("update patient_vitals set sp02=? where pid=?;");
                                            BoundStatement boundStatement_spUpdate = new BoundStatement(result_update_info);
                                            ResultSet resultset_spUpdate = session.execute(boundStatement_spUpdate.bind(spUpdate, findID2));
                                            cluster.close();
                                            System.out.println("Update for SP02 Complete!");
                                        } else {
                                            System.out.println("Canceling Update!");
                                        }


                                    } else {
                                        System.out.println("Looks like this patient does not exist. please refer to the Main Menu to add new patient!\n");
                                    }
                                }


                                break;
                            case 3:
                                System.out.println("What is the name/patient ID created of the Patient that needs updating: \n");
                                System.out.println("Name:");
                                Scanner caseThree_sc7 = new Scanner(System.in);
                                String findName3 = caseThree_sc7.nextLine();
                                System.out.println("Patient ID:");
                                Scanner caseThree_sc8 = new Scanner(System.in);
                                Integer findID3 = caseThree_sc8.nextInt();
                                cluster = Cluster.builder().addContactPoint("localhost").build();
                                session = (Session) cluster.connect("medical_db");
                                PreparedStatement result_nameFind3 = session.prepare("select pid from patient_activity where name=? ALLOW FILTERING;");
                                BoundStatement boundStatement_name3 = new BoundStatement(result_nameFind3);
                                ResultSet resultset_namefind3 = session.execute(boundStatement_name3.bind(findName3));
                                for (Row row : resultset_namefind3) {
                                    Integer pidFind = row.getInt("pid");
                                    cluster.close();
                                    if (findID3.equals(pidFind)) {
                                        Thread.sleep(1000);
                                        System.out.println("Data found for Patient!");
                                        Thread.sleep(2000);
                                        System.out.println("Let's Update the record: \n");
                                        System.out.println("Which record are we updating? \n");
                                        System.out.println("Exercise Duration (1) \n");
                                        System.out.println("Last Activity (2) \n");
                                        System.out.println("Time of Completion(3) \n");
                                        System.out.println("Cancel (0) \n");
                                        Scanner caseThree_sc6 = new Scanner(System.in);
                                        Integer input5 = caseThree_sc6.nextInt();
                                        if (input5 == 1) {
                                            System.out.println("Please enter Exercise Duration(min X.X):");
                                            Scanner sc4 = new Scanner(System.in);
                                            Double dUpdate = sc4.nextDouble();
                                            System.out.println("User input: " + dUpdate);
                                            cluster = Cluster.builder().addContactPoint("localhost").build();
                                            session = (Session) cluster.connect("medical_db");
                                            PreparedStatement result_update_info = session.prepare("update patient_activity set duration=? where pid=?;");
                                            BoundStatement boundStatement_updateDuration = new BoundStatement(result_update_info);
                                            ResultSet resultset_bpUpdate = session.execute(boundStatement_updateDuration.bind(dUpdate, findID3));
                                            cluster.close();
                                            System.out.println("Update for Exercise Duration Complete!");
                                        } else if (input5 == 2) {
                                            System.out.println("Please enter name of Last Activity:");
                                            Scanner sc4 = new Scanner(System.in);
                                            String actUpdate = sc4.nextLine();
                                            System.out.println("User input: " + actUpdate);
                                            cluster = Cluster.builder().addContactPoint("localhost").build();
                                            session = (Session) cluster.connect("medical_db");
                                            PreparedStatement result_update_info = session.prepare("update patient_activity set last_activity=? where pid=?;");
                                            BoundStatement boundStatement_actUpdate = new BoundStatement(result_update_info);
                                            ResultSet resultset_actUpdate = session.execute(boundStatement_actUpdate.bind(actUpdate, findID3));
                                            cluster.close();
                                            System.out.println("Update for Last Activity Complete!");
                                        } else if (input5 == 3) {
                                            System.out.println("Please enter Time of Completion(YYYY-MM-DD):");
                                            Scanner sc4 = new Scanner(System.in);
                                            String timeUpdate = sc4.nextLine();
                                            System.out.println("User input: " + timeUpdate);
                                            cluster = Cluster.builder().addContactPoint("localhost").build();
                                            session = (Session) cluster.connect("medical_db");
                                            PreparedStatement result_update_info = session.prepare("update patient_activity set time=? where pid=?;");
                                            BoundStatement boundStatement_timeUpdate = new BoundStatement(result_update_info);
                                            ResultSet resultset_timeUpdate = session.execute(boundStatement_timeUpdate.bind(timeUpdate, findID3));
                                            cluster.close();
                                            System.out.println("Update for Date Captured!");
                                        } else {
                                            System.out.println("Canceling Update!");
                                        }


                                    } else {
                                        System.out.println("Looks like this patient does not exist. please refer to the Main Menu to add new patient!\n");
                                    }
                                }
                                break;
                            case 0:
                                System.out.println("Returning to Main Menu!");
                        }

                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    Thread.sleep(5000);
                }
                case 0 -> {
                    System.out.println("Exiting! GoodBye!");
                    input = 0;
                }
            }

        }
    }
}
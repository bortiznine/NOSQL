
import com.datastax.driver.core.*;
import org.apache.cassandra.serializers.TimestampSerializer;

import java.util.Scanner;


public class Main {

    public static void main(String[] args) {

        int input=3;
        while (input!=0) {
            System.out.println("Welcome to The Medical DB");
            System.out.println("Are you a entering a New Patient(1), Or looking up Previous Patient(2), Exit (0)");
            Scanner sc = new Scanner(System.in);
            System.out.println("Enter a value :");
            input = sc.nextInt();
            System.out.println("User input: " + input);
            switch (input) {
                case 1:
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

                        cluster.close();
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                    break;
                case 2:
                    System.out.println("Please Select a Query to run to find data:\n\n");
                    System.out.println("Find all patients by name with heart rates higher than 90 BPM(1) \n");
                    System.out.println("Find all patients by Patient ID, name, and address that did cardio last activity(2) \n");

                    Scanner caseTwo_sc1 = new Scanner(System.in);
                    Integer input2 = caseTwo_sc1.nextInt();
                    System.out.println("User input: " + input2);
                    switch (input2){
                        case 1:

                            Cluster cluster;
                            Session session;
                            //Connect to the cluster and Keyspace ecommerce
                            cluster = Cluster.builder().addContactPoint("localhost").build();
                            session = (Session) cluster.connect("medical_db");
                            ResultSet resultSet= session.execute("select name from patient_vitals where heartrate>=90 ALLOW FILTERING;");
                            for (Row row_q1 : resultSet) {
                            System.out.println(row_q1.getString("name"));
                            cluster.close();
                            }
                        break;
                        case 2:

                            //Connect to the cluster and Keyspace ecommerce
                            cluster = Cluster.builder().addContactPoint("localhost").build();
                            session = (Session) cluster.connect("medical_db");
                            ResultSet resultSet2= session.execute("select p1.pid,p1.name,p2.address from patient_activity AS p1, patient_personal_info AS p2 where p1.name=p2.name AND p1.last_activity='cardio' ALLOW FILTERING;");
                            for (Row row_q2 : resultSet2) {
                                System.out.println(row_q2.getInt("p1.pid")+row_q2.getString("p1.name")+row_q2.getString("p2.address"));
                            }
                            cluster.close();
                            break;


                    }
                    case 0:
                        System.out.println("Exiting! GoodBye!");
                        input=0;

            }

        }
    }
}
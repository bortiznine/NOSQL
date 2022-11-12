
import com.datastax.driver.core.*;
import org.apache.cassandra.serializers.TimestampSerializer;

import java.util.Scanner;


public class Main {

    public static void main(String[] args) {
        System.out.println("Welcome to The Medical DB");
        System.out.println("Are you a entering a New Patient(1), Or looking up Previous Patient(2), Exit (0)");
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter a value :");
        String str = sc.nextLine();
        System.out.println("User input: " + str);
        switch (Integer.parseInt(str)) {
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
    System.out.println("Insert Successful");

    PreparedStatement result2 = session.prepare("select * from patient_personal_info where name=? ALLOW FILTERING");
    BoundStatement boundStatement2 = new BoundStatement(result2);
    ResultSet resultset= session.execute(boundStatement2.bind(name));
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

                    cluster.close();

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

                    System.out.println("Cassandra Java Connection");
                    //Connect to the cluster and Keyspace ecommerce
                    cluster = Cluster.builder().addContactPoint("localhost").build();
                    session = (Session) cluster.connect("medical_db");

                    PreparedStatement result_activity = session.prepare("INSERT INTO patient_activity (pid, duration,last_activity, name,time) VALUES (?,?,?,?,?);");
                    BoundStatement boundStatement_activity = new BoundStatement(result_activity);
                    ResultSet results_activity = session.execute(boundStatement_activity.bind(
                            pid, activity_duration, activity, name, date_created));
                    System.out.println("Insert Successful");

                    PreparedStatement result4 = session.prepare("select * from patient_activity where name=? ALLOW FILTERING;");
                    BoundStatement boundStatement4 = new BoundStatement(result4);
                    ResultSet resultset_activity = session.execute(boundStatement4.bind(name));
                    for (Row row_activity : resultset_activity) {
                        pid = row_activity.getInt("pid");
                        activity_duration = row_activity.getDouble("duration");
                        activity = row_activity.getString("last_activity");
                        name = row_activity.getString("name");


                        System.out.println("Patient ID: " + pid);
                        System.out.println("Duration: " + activity_duration);
                        System.out.println("Last Activity: " + activity);
                        System.out.println("Name: " + name);
                        System.out.println("Time Stamp: " + date_created);


                    }
                }
    cluster.close();
}
catch (Exception e){
    System.out.println(e);
}
                break;
            case 2:

        }
    }
}
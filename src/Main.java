
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
    System.out.println("Cassandra Java Connection");
    Cluster cluster;
    Session session;
    //Connect to the cluster and Keyspace ecommerce
    cluster = Cluster.builder().addContactPoint("localhost").build();
    session = (Session) cluster.connect("medical_db");

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


    PreparedStatement result = session.prepare("INSERT INTO patient_personal_info (pid, address,date_created,insurance, name) VALUES (?,?,?,?,?);");
    BoundStatement boundStatement = new BoundStatement(result);
    ResultSet results = session.execute(boundStatement.bind(
            pid, address, date_created, insurance, name));

    PreparedStatement result2 = session.prepare("select * from patient_personal_info where name=? ALLOW FILTERING");
    BoundStatement boundStatement2 = new BoundStatement(result2);
    ResultSet resultset= session.execute(boundStatement2.bind(name));
                for (Row row : resultset) {
                    pid = row.getInt("pid");
                    address = row.getString("address");
                    insurance = row.getString("insurance");
                    name = row.getString("name");
//                    TimestampSerializer = row.getTimestamp("date_created");



                    System.out.println("Patient ID: " + pid);
                    System.out.println("Name: " + name);
                    System.out.println("Address: " + address);
                    System.out.println("Insurance: " + insurance);
                    System.out.println("Dated Created: "+ date_created);



                }
    cluster.close();
}
catch (Exception e){
    System.out.println(e);
}
                break;
        }
    }
}
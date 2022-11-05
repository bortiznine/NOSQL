
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        System.out.println("Welcome to The Medical DB");
        System.out.println("Are you a entering a New Patient(1), Or looking up Previous Patient(2)");
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
                String address = null, insurance = null, name = null;
                Integer pid = null;
                System.out.println("Inserting Data in Cassandra");
                session.execute("INSERT INTO patient_personal_info (pid, address, insurance, name) VALUES (2,'3236 W Tom Street', 'Metlife Advantage', 'Tim')");

                ResultSet resultset = session.execute("select * from patient_personal_info where pid=1");
                for (Row row : resultset) {
                    pid = row.getInt("pid");
                    address = row.getString("address");
                    insurance = row.getString("insurance");
                    name = row.getString("name");


                    System.out.println("Patient ID: " + pid);
                    System.out.println("Name: " + name);
                    System.out.println("Address: " + address);
                    System.out.println("Insurance: " + insurance);


                }
                cluster.close();
            } catch (Exception e) {
                System.out.println(e);
            }

        }
    }
}
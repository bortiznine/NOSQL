
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class Main {

    public static void main(String args[]) {
        System.out.println("Cassandra Java Connection");
        Cluster cluster;
        Session session;
        //Connect to the cluster and Keyspace ecommerce
        cluster = Cluster.builder().addContactPoint("localhost").build();
        session = (Session) cluster.connect("ecommerce");
        System.out.println("Inserting Data in Cassandra");
        session.execute("INSERT INTO products (pdt_id, cat_id, pdt_name, pdt_desc, price, shipping) VALUES (001,104, 'Danby 0.7 cu. ft. Microwave Oven', 'Capacity of 0.7 cu. ft.10 different power levels', 54.00, 'Expedited')");
        String pdtid = null, pdtname = null, pdtdesc = null;
        float price = 0;
        ResultSet resultset = session.execute("select * from products where pdt_id=005");
        for (Row row : resultset) {
            pdtid = Integer.toString(row.getInt("pdt_id"));
            pdtname = row.getString("pdt_name");
            pdtdesc = row.getString("pdt_desc");
            price = row.getFloat("price");


            System.out.println("Product ID: " + pdtid);
            System.out.println("Name: " + pdtname);
            System.out.println("Description: " + pdtdesc);
            System.out.println("Price: " + price);


        }
        cluster.close();
    }
}
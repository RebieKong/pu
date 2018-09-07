import java.io.*;
import java.sql.*;

public class KKK {

    private static Connection connection;
    private static Connection connection2;

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection = DriverManager
                    .getConnection("jdbc:postgresql://192.168.30.173:5432/gis3",
                            "postgres", "postgres");
            connection2 = DriverManager
                    .getConnection("jdbc:postgresql://192.168.30.173:5432/gis",
                            "postgres", "postgres");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String... args) throws IOException {

        // CHANGE HERE
        String input = "";
        String output = "";


        BufferedReader bufferedReader = new BufferedReader(new FileReader(input));
        FileWriter fw = new FileWriter(output);
        bufferedReader.lines().forEach(s -> {
            String[] d = s.split(",");
//            if (Integer.valueOf(id)>145743){
            double lat = Double.valueOf(d[4]);
            double lon = Double.valueOf(d[5]);
            try {
                fw.write(s);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                System.out.println(s);
                fw.write("," + find2(lat, lon) + "\n");
            } catch (SQLException | IOException e) {
                e.printStackTrace();
            }
            try {
                fw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
//            }
        });
//        String v = "SELECT * FROM (SELECT ST_CONTAINS(wkb_geometry,ST_POINT(114.5890578577185,24.20292799872379)),name FROM hy) AS p WHERE st_contains='t';";
    }

    public static String find(double lat, double lon) throws SQLException {
        StringBuffer result = new StringBuffer();
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM (SELECT ST_CONTAINS(wkb_geometry,ST_POINT(?,?)) as ext,name FROM dt) as q where ext='t'");
        ps.setDouble(1, lat);
        ps.setDouble(2, lon);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            String qk = rs.getString(2);
            PreparedStatement stm = connection2.prepareStatement("SELECT * FROM info WHERE code=?");
            stm.setString(1, qk);
            ResultSet v = stm.executeQuery();
            if (v.next()) {
                result.append(v.getString(1)).append(",");
                result.append(v.getString(2));
            } else {
                System.out.println("boom2");
            }
        } else {
            System.out.println("boom1");
        }
        return result.toString();
    }

    public static String find2(double lat, double lon) throws SQLException {
        StringBuilder result = new StringBuilder();
        PreparedStatement ps1 = connection.prepareStatement("SELECT * FROM (SELECT ST_CONTAINS(wkb_geometry,ST_POINT(?,?)) as ext,nl_name_2,id_1,id_2 FROM chn_adm2 where id_1=6) as q where ext='t'");
        ps1.setDouble(1, lat);
        ps1.setDouble(2, lon);
        ResultSet rs1 = ps1.executeQuery();
        if (rs1.next()) {
            String qk = rs1.getString(2);
            if (qk != null && qk.contains("|")) {
                qk = qk.split("\\|")[0];
            }
            result.append(rs1.getString(3));
            result.append(",");
            result.append(rs1.getString(4));
            result.append(",");
            result.append(qk);
        } else {
            PreparedStatement tps1 = connection.prepareStatement("SELECT * FROM (SELECT ST_CONTAINS(wkb_geometry,ST_POINT(?,?)) as ext,nl_name_2,id_1,id_2 FROM chn_adm2) as q where ext='t'");
            tps1.setDouble(1, lat);
            tps1.setDouble(2, lon);
            ResultSet trs1 = tps1.executeQuery();
            if (trs1.next()) {
                String qk = trs1.getString(2);

                if (qk != null && qk.contains("|")) {
                    qk = qk.split("\\|")[0];
                }
                result.append(trs1.getString(3));
                result.append(",");
                result.append(trs1.getString(4));
                result.append(",");
                result.append(qk);
            } else {
                System.out.println("boom1");
            }
        }


//        PreparedStatement ps2 = connection.prepareStatement("SELECT * FROM (SELECT ST_CONTAINS(wkb_geometry,ST_POINT(?,?)) as ext,nl_name_3 FROM chn_adm3 where id_1=6) as q where ext='t'");
//
//        ps2.setDouble(1, lat);
//        ps2.setDouble(2, lon);
//        ResultSet rs2 = ps2.executeQuery();
//        if (rs2.next()) {
//            String qk = rs2.getString(2);
//            if (qk != null && qk.contains("\\|")) {
//                qk = qk.split("\\|")[0];
//            }
//            result.append(",").append(qk);
//        } else {
//            PreparedStatement tps1 = connection.prepareStatement("SELECT * FROM (SELECT ST_CONTAINS(wkb_geometry,ST_POINT(?,?)) as ext,nl_name_3 FROM chn_adm3) as q where ext='t'");
//            tps1.setDouble(1, lat);
//            tps1.setDouble(2, lon);
//            ResultSet trs1 = tps1.executeQuery();
//            if (trs1.next()) {
//                String qk = trs1.getString(2);
//                if (qk != null && qk.contains("\\|")) {
//                    qk = qk.split("\\|")[0];
//                }
//                result.append(",").append(qk);
//            } else {
//                System.out.println("boom1");
//            }
//        }
        return result.toString();
    }

}

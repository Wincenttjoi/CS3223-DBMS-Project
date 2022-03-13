package embedded;
import java.sql.*;

import simpledb.jdbc.embedded.EmbeddedDriver;

public class CreateStudentDBDemo {
   public static void main(String[] args) {
      Driver d = new EmbeddedDriver();
      String url = "jdbc:simpledb:demodb";

      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
         String s = "create table STUDENT(SId int, SName varchar(30), MajorId int, GradYear int)";
         stmt.executeUpdate(s);
         System.out.println("Table STUDENT created.");
         
         // Create index
         s = "create index majorid_idx on student (majorid) using btree";
         stmt.executeUpdate(s);
         System.out.println("Majorid index created");
         
         s = "create index studentid_idx on student (SId) using hash";
         stmt.executeUpdate(s);
         System.out.println("Sid index created");

         s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = {
        		 "(1, 'Rick Werner', 400, 2017)",
        		 "(2, 'Shiloh Hartman', 310, 2021)",
        		 "(3, 'Damon Kim', 430, 2020)",
        		 "(4, 'Cerys Armstrong', 110, 2018)",
        		 "(5, 'Layla-Mae Barker', 240, 2016)",
        		 "(6, 'Tevin Wallace', 430, 2016)",
        		 "(7, 'Kamile Wilkins', 430, 2021)",
        		 "(8, 'Rohaan Keenan', 200, 2016)",
        		 "(9, 'Demi-Leigh Gamble', 300, 2016)",
        		 "(10, 'Lilly-Ann Joyce', 10, 2018)",
        		 "(11, 'Jadene Oneil', 350, 2017)",
        		 "(12, 'Samad Hawkins', 40, 2016)",
        		 "(13, 'Scott Goulding', 10, 2020)",
        		 "(14, 'Riley-Jay Freeman', 40, 2019)",
        		 "(15, 'Sayed Marsh', 100, 2020)",
        		 "(16, 'Amiya Caldwell', 200, 2017)",
        		 "(17, 'Huzaifa Wiley', 20, 2017)",
        		 "(18, 'Jemima Davey', 250, 2016)",
        		 "(19, 'Rhia Bird', 110, 2019)",
        		 "(20, 'Romy Delaney', 90, 2021)",
        		 "(21, 'Dania Kerr', 90, 2017)",
        		 "(22, 'Milana Barrera', 470, 2020)",
        		 "(23, 'Ilyas Dudley', 150, 2019)",
        		 "(24, 'Aqsa Rivera', 350, 2019)",
        		 "(25, 'Nana Lott', 350, 2021)",
        		 "(26, 'Darci Hodson', 100, 2021)",
        		 "(27, 'Weronika Bain', 380, 2019)",
        		 "(28, 'Callan Derrick', 30, 2016)",
        		 "(29, 'Otis Devlin', 50, 2017)",
        		 "(30, 'Millie Curtis', 480, 2020)",
        		 "(31, 'Sanah Morrison', 410, 2018)",
        		 "(32, 'Rania Alcock', 140, 2017)",
        		 "(33, 'Jay Pike', 450, 2020)",
        		 "(34, 'Misha Mendez', 150, 2016)",
        		 "(35, 'Marcos Whitley', 300, 2016)",
        		 "(36, 'Tierney Mcdonnell', 170, 2016)",
        		 "(37, 'Maiya Rhodes', 290, 2017)",
        		 "(38, 'Darcie-Mae Winters', 20, 2019)",
        		 "(39, 'Marwah Yu', 250, 2021)",
        		 "(40, 'Aniela Suarez', 120, 2019)",
        		 "(41, 'Corinne Hill', 320, 2021)",
        		 "(42, 'Philippa Nicholls', 300, 2017)",
        		 "(43, 'Elif Mayo', 200, 2017)",
        		 "(44, 'Tonisha Strickland', 150, 2018)",
        		 "(45, 'Wilfred Betts', 460, 2016)",
        		 "(46, 'Cassius Spence', 390, 2019)",
        		 "(47, 'Amiee Oliver', 180, 2017)",
        		 "(48, 'Imaad Wickens', 20, 2019)",
        		 "(49, 'Caio Cottrell', 290, 2018)",
        		 "(50, 'Ethan Bate', 120, 2017)"
               };
         for (int i=0; i<studvals.length; i++)
            stmt.executeUpdate(s + studvals[i]);
         System.out.println("STUDENT records inserted.");

         s = "create table DEPT(DId int, DName varchar(30))";
         stmt.executeUpdate(s);
         System.out.println("Table DEPT created.");

         s = "insert into DEPT(DId, DName) values ";
         String[] deptvals = {
        		 "(10, 'Law')",
        		 "(20, 'Law')",
        		 "(30, 'Law')",
        		 "(40, 'Law')",
        		 "(50, 'Law')",
        		 "(60, 'Accounting')",
        		 "(70, 'Accounting')",
        		 "(80, 'Accounting')",
        		 "(90, 'Accounting')",
        		 "(100, 'Accounting')",
        		 "(110, 'Music')",
        		 "(120, 'Music')",
        		 "(130, 'Music')",
        		 "(140, 'Music')",
        		 "(150, 'Music')",
        		 "(160, 'Real Estate')",
        		 "(170, 'Real Estate')",
        		 "(180, 'Real Estate')",
        		 "(190, 'Real Estate')",
        		 "(200, 'Real Estate')",
        		 "(210, 'Computer Science')",
        		 "(220, 'Computer Science')",
        		 "(230, 'Computer Science')",
        		 "(240, 'Computer Science')",
        		 "(250, 'Computer Science')",
        		 "(260, 'Marketing')",
        		 "(270, 'Marketing')",
        		 "(280, 'Marketing')",
        		 "(290, 'Marketing')",
        		 "(300, 'Marketing')",
        		 "(310, 'Industrial Design')",
        		 "(320, 'Industrial Design')",
        		 "(330, 'Industrial Design')",
        		 "(340, 'Industrial Design')",
        		 "(350, 'Industrial Design')",
        		 "(360, 'Economics')",
        		 "(370, 'Economics')",
        		 "(380, 'Economics')",
        		 "(390, 'Economics')",
        		 "(400, 'Economics')",
        		 "(410, 'Chemical Engineering')",
        		 "(420, 'Chemical Engineering')",
        		 "(430, 'Chemical Engineering')",
        		 "(440, 'Chemical Engineering')",
        		 "(450, 'Chemical Engineering')",
        		 "(460, 'Mechanical Engineering')",
        		 "(470, 'Mechanical Engineering')",
        		 "(480, 'Mechanical Engineering')",
        		 "(490, 'Mechanical Engineering')",
        		 "(500, 'Mechanical Engineering')"
                              };
         for (int i=0; i<deptvals.length; i++)
            stmt.executeUpdate(s + deptvals[i]);
         System.out.println("DEPT records inserted.");

         s = "create table COURSE(CId int, Title varchar(40), DeptId int)";
         stmt.executeUpdate(s);
         System.out.println("Table COURSE created.");

         s = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = {
        		 "(12, 'Criminal Law', 10)",
        		 "(22, 'law of Torts', 20)",
        		 "(32, 'The Law of Contract', 30)",
        		 "(42, 'Land Lawa', 40)",
        		 "(52, 'Equity and Trusts', 50)",
        		 "(62, 'Business Law', 60)",
        		 "(72, 'Financial Markets', 70)",
        		 "(82, 'Taxation', 80)",
        		 "(92, 'Microeconomics', 90)",
        		 "(102, 'Corporate Finance', 100)",
        		 "(112, 'Introductory Music', 110)",
        		 "(122, 'Composition and Theory', 120)",
        		 "(132, 'Performance', 130)",
        		 "(142, 'Music and Media', 140)",
        		 "(152, 'Samplings', 150)",
        		 "(162, 'Residential Real Estate', 160)",
        		 "(172, 'Legal', 170)",
        		 "(182, 'RE Taxes', 180)",
        		 "(192, 'RE Technology', 190)",
        		 "(202, 'Land Use and Property Rights', 200)",
        		 "(212, 'Operating System', 210)",
        		 "(222, 'Data Structure', 220)",
        		 "(232, 'Algorithm', 230)",
        		 "(242, 'Basic Programming', 240)",
        		 "(252, 'Databases', 250)",
        		 "(262, 'Market Research', 260)",
        		 "(272, 'Advertising', 260)",
        		 "(282, 'Consumer Behaviour', 260)",
        		 "(292, 'Public Relations', 260)",
        		 "(302, 'Quantitative Methods', 260)",
        		 "(312, 'Digital Marketing', 260)",
        		 "(322, 'Marketing Ethics', 260)",
        		 "(332, 'Decision Science', 260)",
        		 "(342, 'B2B Marketing', 270)",
        		 "(352, 'Principles of Finance', 270)",
        		 "(362, 'Sustainable Marketing', 270)",
        		 "(372, 'Retail Management', 270)",
        		 "(382, 'Brand Management', 270)",
        		 "(392, 'Marketing Research', 270)",
        		 "(402, 'IDE Design', 310)",
        		 "(412, 'IDE Drawing', 310)",
        		 "(422, 'IDE Theory', 310)",
        		 "(432, 'Econometrics', 360)",
        		 "(442, 'Economic Policy', 360)",
        		 "(452, 'Legal Studies', 360)",
        		 "(462, 'Polymer Engineering', 430)",
        		 "(472, 'Process Control', 430)",
        		 "(482, 'Chemical Process Optimization', 430)",
        		 "(492, 'Mechanics of Machines', 480)",
        		 "(502, 'Fluid Mechanics', 490)",
        		 "(512, 'Thermodynamics', 500)"
                                };
         for (int i=0; i<coursevals.length; i++)
            stmt.executeUpdate(s + coursevals[i]);
         System.out.println("COURSE records inserted.");

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         stmt.executeUpdate(s);
         System.out.println("Table SECTION created.");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = {
        		 "(13, 102, 'Moon Geonsik', 2020)",
        		 "(23, 222, 'Kelvin Wong', 2021)",
        		 "(33, 242, 'Wincent Tjoi', 2016)",
        		 "(43, 112, 'Dina Warner', 2016)",
        		 "(53, 452, 'Jan Stott', 2020)",
        		 "(63, 472, 'Garfield Hebert', 2016)",
        		 "(73, 162, 'Ayaz Horner', 2020)",
        		 "(83, 382, 'Heena Herman', 2020)",
        		 "(93, 132, 'Hakim Collins', 2021)",
        		 "(103, 442, 'Janae Gillespie', 2019)",
        		 "(113, 502, 'Raihan Baxter', 2018)",
        		 "(123, 52, 'Zaina Myers', 2021)",
        		 "(133, 372, 'Abdurahman Holman', 2019)",
        		 "(143, 472, 'Cole Li', 2016)",
        		 "(153, 392, 'Shelbie Knowles', 2020)",
        		 "(163, 162, 'Stephan Leal', 2019)",
        		 "(173, 102, 'Sheikh Rowe', 2018)",
        		 "(183, 12, 'Korban Fleming', 2019)",
        		 "(193, 32, 'Layan Bonilla', 2020)",
        		 "(203, 72, 'Drew Mills', 2019)",
        		 "(213, 102, 'Amara Tanner', 2019)",
        		 "(223, 452, 'Kaydee Parks', 2021)",
        		 "(233, 12, 'Gracey Peacock', 2017)",
        		 "(243, 52, 'Rosina Lucero', 2019)",
        		 "(253, 462, 'Kelise Berger', 2018)",
        		 "(263, 72, 'Marie Farrell', 2021)",
        		 "(273, 362, 'Gabija Baird', 2021)",
        		 "(283, 82, 'Riyad Avila', 2021)",
        		 "(293, 182, 'Ifan Squires', 2019)",
        		 "(303, 252, 'Rohan Drew', 2018)",
        		 "(313, 112, 'Beatriz Shields', 2017)",
        		 "(323, 132, 'Sianna Reyes', 2016)",
        		 "(333, 22, 'Alisa David', 2020)",
        		 "(343, 512, 'Maya Tucker', 2016)",
        		 "(353, 382, 'Alaya Frost', 2019)",
        		 "(363, 142, 'Beatrix Oneal', 2018)",
        		 "(373, 402, 'Nellie Wiggins', 2019)",
        		 "(383, 302, 'Haidar Merrill', 2017)",
        		 "(393, 332, 'Ted Cullen', 2016)",
        		 "(403, 252, 'Chase Wyatt', 2019)",
        		 "(413, 242, 'Zakariyah Power', 2019)",
        		 "(423, 62, 'Kade Ward', 2020)",
        		 "(433, 52, 'Jibril Ali', 2016)",
        		 "(443, 202, 'Charlie Dixon', 2019)",
        		 "(453, 252, 'Ali Heath', 2020)",
        		 "(463, 372, 'Cecil Derrick', 2017)",
        		 "(473, 262, 'Poppy-Mae Boyle', 2021)",
        		 "(483, 252, 'Ritchie Cairns', 2021)",
        		 "(493, 72, 'Moon Geonsik', 2020)",
        		 "(503, 502, 'Kelvin Wong', 2020)",
        		 "(513, 372, 'Wincent Tjoi', 2017)"
                              };
         for (int i=0; i<sectvals.length; i++)
            stmt.executeUpdate(s + sectvals[i]);
         System.out.println("SECTION records inserted.");

         s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         stmt.executeUpdate(s);
         System.out.println("Table ENROLL created.");

         s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = {
        		 "(14, 33, 253,'C')",
        		 "(24, 43, 73,'B')",
        		 "(34, 26, 173,'A-')",
        		 "(44, 10, 183,'B-')",
        		 "(54, 8, 163,'B-')",
        		 "(64, 40, 93,'A+')",
        		 "(74, 24, 373,'A-')",
        		 "(84, 38, 433,'A')",
        		 "(94, 3, 253,'A-')",
        		 "(104, 1, 103,'B-')",
        		 "(114, 39, 303,'C')",
        		 "(124, 49, 353,'B-')",
        		 "(134, 4, 93,'B-')",
        		 "(144, 13, 233,'B+')",
        		 "(154, 22, 113,'C')",
        		 "(164, 43, 293,'C')",
        		 "(174, 38, 193,'C+')",
        		 "(184, 24, 163,'B')",
        		 "(194, 42, 353,'B')",
        		 "(204, 35, 353,'A+')",
        		 "(214, 6, 253,'B+')",
        		 "(224, 9, 463,'A+')",
        		 "(234, 20, 263,'C+')",
        		 "(244, 22, 113,'A+')",
        		 "(254, 22, 343,'A')",
        		 "(264, 30, 503,'A+')",
        		 "(274, 14, 233,'A-')",
        		 "(284, 45, 113,'A-')",
        		 "(294, 36, 293,'C+')",
        		 "(304, 28, 183,'B')",
        		 "(314, 40, 313,'B+')",
        		 "(324, 46, 53,'A-')",
        		 "(334, 7, 253,'A-')",
        		 "(344, 49, 513,'B')",
        		 "(354, 5, 33,'B')",
        		 "(364, 49, 383,'B-')",
        		 "(374, 41, 373,'A-')",
        		 "(384, 5, 303,'A+')",
        		 "(394, 32, 363,'A-')",
        		 "(404, 15, 423,'B')",
        		 "(414, 11, 373,'A+')",
        		 "(424, 42, 393,'C+')",
        		 "(434, 39, 303,'B')",
        		 "(444, 40, 13,'A')",
        		 "(454, 32, 43,'B+')",
        		 "(464, 5, 23,'C')",
        		 "(474, 17, 333,'B-')",
        		 "(484, 9, 273,'C')",
        		 "(494, 30, 113,'B-')",
        		 "(504, 15, 283,'A+')",
        		 "(514, 36, 163,'C+')"
                                };
         for (int i=0; i<enrollvals.length; i++)
            stmt.executeUpdate(s + enrollvals[i]);
         System.out.println("ENROLL records inserted.");
         
       
         
      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}

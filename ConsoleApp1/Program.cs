using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic;
using System.Reflection;
using System.Collections.ObjectModel;
using System.Data.SqlClient;

namespace ReadingDataFromCSV
{
    class Program
    {
        static void Main(string[] args)
        {

            
            CvsToSqlInsert();
            Console.ReadLine();
        }
        static void CvsToSqlInsert()
        {
            string address = "C:\\Users\\Sefa\\Desktop\\covid19_vaccination_With_SQL\\ConsoleApp1\\country_vaccination_stats.csv";//changeable
            var lines = File.ReadAllLines(address);
            var list = new List<Contact>();
            for (int i = 1; i < lines.Length; i++)
            {
                var values = lines[i].Split(',');
                if (values.Length == 4)
                {
                    var contact = new Contact() { Country = values[0], Date = Convert.ToDateTime(values[1], CultureInfo.InvariantCulture), Daily_vaccinations = (values[2] == "") ? 0 : Convert.ToInt32(values[2]), Vaccines = values[3] };
                    list.Add(contact);
                }
                else if (values.Length == 5)
                {
                    var contact = new Contact() { Country = values[0], Date = Convert.ToDateTime(values[1], CultureInfo.InvariantCulture), Daily_vaccinations = (values[2] == "") ? 0 : Convert.ToInt32(values[2]), Vaccines = values[3] + "," + values[4] };
                    list.Add(contact);
                }
                else if (values.Length == 6)
                {
                    var contact = new Contact() { Country = values[0], Date = Convert.ToDateTime(values[1], CultureInfo.InvariantCulture), Daily_vaccinations = (values[2] == "") ? 0 : Convert.ToInt32(values[2]), Vaccines = values[3] + "," + values[4] + "," + values[5] };
                    list.Add(contact);
                }
            }


            var All_County_Median = new List<Country_Median>();
            foreach (var x in list)
            {
                int check = 0;
                foreach (var y in All_County_Median)
                {
                    if (y.Country == x.Country)
                        check = 1;

                }
                if (check == 0)
                {
                    var newadd = new Country_Median() { Country = x.Country, Median_Daily_vaccinations = 0 };
                    All_County_Median.Add(newadd);
                }

            }



            List<int> Country_Daily_vaccinations = new List<int>();
            int counter = 0;
            foreach (var x in All_County_Median)
            {


                Country_Daily_vaccinations.Clear();
                counter = 0;
                foreach (var y in list)
                {
                    if (x.Country == y.Country)
                    {
                        if (y.Daily_vaccinations != 0)
                        {
                            Country_Daily_vaccinations.Add(y.Daily_vaccinations);
                            counter++;
                        }

                    }

                }

                Country_Daily_vaccinations.Sort();

                //for (int i = 0; i < counter; i++)
                //    Console.WriteLine(Country_Daily_vaccinations[i].ToString());
                //Console.WriteLine();
                //Console.WriteLine();



                if (Country_Daily_vaccinations.Count % 2 == 0 && counter > 0)
                {
                    x.Median_Daily_vaccinations = ((Country_Daily_vaccinations[counter / 2] + Country_Daily_vaccinations[(counter / 2) - 1]) / 2);
                    //Console.WriteLine("cift :" + x.Median_Daily_vaccinations.ToString());

                }
                else if (counter == 1)
                {
                    x.Median_Daily_vaccinations = Country_Daily_vaccinations[counter - 1];
                    //Console.WriteLine("bir :" + x.Median_Daily_vaccinations.ToString());
                }
                else if (counter == 0)
                {
                    //Console.WriteLine("YOK YOK");
                }
                else
                {
                    x.Median_Daily_vaccinations = Country_Daily_vaccinations[counter / 2];
                    //Console.WriteLine("tek :" + x.Median_Daily_vaccinations.ToString());
                }


            }


            //database jobs start here
            SqlConnection con = new SqlConnection(@"Data Source=.\SQLEXPRESS;Initial Catalog=covid19;Integrated Security=True");
            con.Open();
            string clear_table_com = "delete from country_vaccination_stats";
            SqlCommand clear_table = new SqlCommand(clear_table_com, con);
            clear_table.ExecuteNonQuery();
            con.Close();

         
            foreach (var x in list)
            {
                if(x.Daily_vaccinations!=0)
                {
                    try
                    {
                    con.Open();
                    string add_com = "insert into country_vaccination_stats values(@country,@data,@daily_vac,@vac) ";
                    SqlCommand addsql = new SqlCommand(add_com, con);
                    addsql.Parameters.AddWithValue("@country", x.Country);
                    addsql.Parameters.AddWithValue("@data", x.Date);
                    addsql.Parameters.AddWithValue("@daily_vac", x.Daily_vaccinations);
                    addsql.Parameters.AddWithValue("@vac", x.Vaccines);
                    addsql.ExecuteNonQuery();
                        con.Close();
                    }
                    catch(SqlException sqlEx)
                    {
                        Console.WriteLine(sqlEx.Message.ToString());
                    }
                }
                else
                {
                    foreach(var y in All_County_Median)
                    {
                        if(x.Country==y.Country)
                        {
                            try
                            {
                                con.Open();
                                string add_com = "insert into country_vaccination_stats values(@country,@data,@daily_vac,@vac) ";
                                SqlCommand addsql = new SqlCommand(add_com, con);
                                addsql.Parameters.AddWithValue("@country", x.Country);
                                addsql.Parameters.AddWithValue("@data", x.Date);
                                addsql.Parameters.AddWithValue("@daily_vac", y.Median_Daily_vaccinations);
                                addsql.Parameters.AddWithValue("@vac", x.Vaccines);
                                addsql.ExecuteNonQuery();
                                con.Close();
                            }
                            catch (SqlException sqlEx)
                            {
                                Console.WriteLine(sqlEx.Message.ToString());
                            }
                        }
                    }
                }
            }

          
            Console.WriteLine("Completed!");
            




        }
        public class Contact
        {
            public string Country { get; set; }
            public DateTime Date { get; set; }
            public int Daily_vaccinations { get; set; }
            public string Vaccines { get; set; }

        }

        public class Country_Median
        {
            public string Country { get; set; }
            public int Median_Daily_vaccinations { get; set; }

        }
    }
}

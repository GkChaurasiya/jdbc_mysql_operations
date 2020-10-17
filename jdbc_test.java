import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;
import java.util.*;

public class jdbc_test {
	 // JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost:3306/user";

	   //  Database credentials
	   static final String USER = "root";
	   static final String PASS = "root67890";
	   public static Connection MakeConnection()
	   {
		   Connection conn = null;
		   try{
			      //STEP 2: Registering JDBC driver
			      Class.forName(JDBC_DRIVER);

			      //STEP 3: Open a connection
			      System.out.println("Connecting to database...");
			      conn = DriverManager.getConnection(DB_URL,USER,PASS);
		   }catch(Exception e)
		   {
			      //Handle errors for Class.forName
			      e.printStackTrace();
		   }
			
		   return conn;
	   }
	   public static boolean checkTheExistenceOfTable(String table_name)
	   {
		   Connection conn=jdbc_test.MakeConnection();
		   Statement stmt = null;
		   try
		   {
		   stmt=conn.createStatement();
	   		String check_query="DESCRIBE "+table_name+";";
	   		ResultSet rs=stmt.executeQuery(check_query);
	   		if(rs.next()==true)
	   			return true;
	   		
		   }catch(SQLException e) {}
		   return false;
	   }
	   @SuppressWarnings("resource")
	public static void main(String[] args)
	   {
	  
	   Statement stmt = null;
	   PreparedStatement pstmt=null;
	   //calling MakeConnection() method
	   		Connection conn=jdbc_test.MakeConnection();
	   
	   // Creating Scanner object to accept users input
	   Scanner sc=new Scanner(System.in);
	   int choice;
	   int i,k,j,p;
	   String check_query;
	   String field_name[]=new String[5];
	   String field_type[]=new String[5];
	   int field_data_int[]=new int[5];
  		String field_data_String[]=new String[5];
  		Float field_data_Float[]=new Float[5];
  		
	   try
	   {
			   do
			   {
				   System.out.println("1. CREATE TABLE");
				   System.out.println("2. INSERT A RECORD");
				   System.out.println("3. UPDATE A RECORD");
				   System.out.println("4. DELETE A RECORD");
				   System.out.println("5. RETRIEVE RECORDS");
				   System.out.println("6. SEARCH A RECORD");
				   System.out.println("0. EXIT");
				   System.out.println("ENTER YOUR CHOICE");
				   choice=sc.nextInt();
				   switch(choice)
				   {
					   case 1:
						   		//Creating a table
						   		System.out.println("Enter the name of the table:");
						   		String table_name=sc.next();
						   		if(jdbc_test.checkTheExistenceOfTable(table_name))
						   			System.out.println("Table already exists!!!");
						   		else
						   		{
								   		System.out.println("Enter number of column  the table:");
								   		int no_of_col=sc.nextInt();
								   		String col_name[]=new String[no_of_col];
								   		String col_type[]=new String[no_of_col];
								   		int validating_pk_status=0;
								   		String query="CREATE TABLE "+table_name+" (";
								   		for(i=0;i<no_of_col;i++)
								   		{
								   			System.out.println("Enter column name:"+(i+1));
								   			col_name[i]=sc.next();
								   			System.out.println("Enter column datetype:"+(i+1));
								   			col_type[i]=sc.next();
								   			if(i==0)
								   			{
									   			System.out.println("Do you want make "+col_name[i]+" as primary key if yes ENTER 1 otherise 0");
									   			validating_pk_status=sc.nextInt();
								   			}
								   			if(validating_pk_status==1 && i==0)
								   				query=query+col_name[i]+" "+col_type[i]+" PRIMARY KEY,";
								   			else if(i<no_of_col-1)
								   				query=query+col_name[i]+" "+col_type[i]+",";
								   			else
								   				query=query+col_name[i]+" "+col_type[i];
								   		}
								   		query=query+")";
								   		System.out.println("Your Query:");
								   		System.out.println(query);
								    
								   		pstmt=conn.prepareStatement(query);
								   		pstmt.execute();
								   		System.out.println("Table created");
						   		}
						   	
						   break;
					   case 2:
							   System.out.println("Enter the name of the table:");
						   	   table_name=sc.next();
						   	   
						   	   
						   	   if(jdbc_test.checkTheExistenceOfTable(table_name))
						   	   {
						   		   System.out.println("Table found!!");
						   	   
								   	stmt=conn.createStatement();
							   		check_query="DESCRIBE "+table_name+";";
							   		ResultSet rs=stmt.executeQuery(check_query);	   		
							   		i=0;
							   		while(rs.next())
							   		{
							         //Retrieve by column name
							         field_name[i] = rs.getString(1);
							         field_type[i] = rs.getString(2);
							           sc.nextLine();
								   		System.out.println("Enter data for "+field_name[i]);
								   		if(field_type[i].contains("int"))
								   		{
								   			field_data_int[i]=sc.nextInt();
								   			sc.nextLine();
								   		}
								   		else if(field_type[i].contains("varchar"))
								   		{
								   			field_data_String[i]=sc.nextLine();
								   		}
								   		else if(field_type[i].contains("float"))
								   		{
								   			field_data_Float[i]=sc.nextFloat();
								   			sc.nextLine();
								   		}
								   		i++;
								   			
								   	}
							   		String check_record_query="SELECT * FROM "+table_name+" WHERE "+field_name[0]+"=?";
							   		pstmt=conn.prepareStatement(check_record_query);
							   		if(field_type[0].contains("int"))
							   			pstmt.setInt(1, field_data_int[0]);
							   		else if(field_type[0].contains("varchar"))
							   			pstmt.setString(1, field_data_String[0]);
							   		else if(field_type[0].contains("float"))
							   			pstmt.setFloat(1, field_data_Float[0]);
							   		 rs=pstmt.executeQuery();
							   		 if(rs.next())
							   		 {
							   			 System.out.println("Record already exist with same "+field_name[0]);
							   			 break;
							   		 }
							   		
							   		String insert_query="INSERT INTO "+table_name+"(";
							   		
							   		
							   		for(k=0;k<i-1;k++)
							   		{
							   			insert_query=insert_query+field_name[k]+",";
							   		}
							   		insert_query=insert_query+field_name[k]+") VALUES (";
							   		
							   		
							   		for(j=0;j<i-1;j++)
							   		{
							   			insert_query=insert_query+"?,";
							   		}
							   		insert_query=insert_query+"?)";
							   		
							   		
							   		System.out.println(insert_query);
							   		pstmt=conn.prepareStatement(insert_query);
							   		p=0;
							   		while(p<i)
							   		{
								   		if(field_type[p].contains("int"))
								   			pstmt.setInt(p+1,field_data_int[p]);
								   		else if(field_type[p].contains("varchar"))
								   			pstmt.setString(p+1,field_data_String[p]);
								   		else if(field_type[p].contains("float"))
								   			pstmt.setFloat(p+1,field_data_Float[p]);
								   		p++;
								   			
								   	}
							   		pstmt.executeUpdate();
							   		System.out.println("Record Inserted Successfully");
						   	   }
						   	   else
						   		   System.out.println("Table not found");
							   		
					   break;
					   case 3:
						   System.out.println("Enter the name of the table:");
					   	   table_name=sc.next();
					   	   
					   	   
					   	   if(jdbc_test.checkTheExistenceOfTable(table_name))
					   	   {
					   		   System.out.println("Table found!!");
					   	   
							   	stmt=conn.createStatement();
						   		check_query="DESCRIBE "+table_name+";";
						   		ResultSet rs=stmt.executeQuery(check_query);	   		
						   		i=0;
						   		while(rs.next())
						   		{
						         //Retrieve by column name
						         field_name[i] = rs.getString(1);
						         field_type[i] = rs.getString(2);
							   		i++;
							   	}
						   		int field_pos=0;
						   		String field_name_toUpdate=null,update_query;
						   		int field_data_toUpdate_int,key_data_int;
						   		String field_data_toUpdate_string,key_data_string;
						   		Float field_data_toUpdate_float,key_data_float;
						   		update_query="UPDATE "+table_name+" SET ";
						   		while(true)
						   		{
						   			j=1;
						   			while(j<i) 
						   			{
						   				System.out.println("Enter "+(j+1)+" to update "+field_name[j]+" otherwise Enter -1 to update another field");
						   				field_pos=sc.nextInt();
						   				if(field_pos>1 && field_pos<=i || j==(i-1))
						   					break;
						   				
						   				j++;
						   			}
						   			if(field_pos<0 && j==(i-1))
						   			{
						   				System.out.println("No other field found!! Exiting from UPDATE MENU");
						   				break;
						   			}
						   			if(field_pos>1 && field_pos<=i)
						   			{
							   			field_name_toUpdate=field_name[field_pos-1];
							   			update_query=update_query+field_name_toUpdate+"=? WHERE "+field_name[0]+"=?;";
							   			System.out.println(update_query);
							   			break;
							   			
						   			}
						   		}

						   		//executing update query
						   		pstmt=conn.prepareStatement(update_query);
						   		System.out.println("Enter "+field_name[0]+" for which you want to update data and the new data for "+field_name_toUpdate);
						   		if(field_type[0].contains("int") || field_type[0].contains("varchar") || field_type[0].contains("float"))
						   		{
						   			// for accepting data primary key and setting that data to ther field after WHERE 
						   			if(field_type[0].contains("int"))
						   			{
						   				key_data_int=sc.nextInt();
						   				pstmt.setInt(2, key_data_int);
						   			}
						   			else if(field_type[0].contains("varchar"))
						   			{
						   				key_data_string=sc.nextLine();
						   				pstmt.setString(2, key_data_string);
					   				}
						   			else if(field_type[0].contains("float"))
						   			{
							   			key_data_float=sc.nextFloat();
							   			pstmt.setFloat(2, key_data_float);
						   			}
						   			// for accepting data for the field that u want to update and setting that data to field after SET as a value
						   			if(field_type[field_pos-1].contains("int"))
						   			{
						   				field_data_toUpdate_int=sc.nextInt();
						   				pstmt.setInt(1, field_data_toUpdate_int);
						   			}
						   			else if(field_type[field_pos-1].contains("varchar"))
						   			{
						   				field_data_toUpdate_string=sc.nextLine();
						   				pstmt.setString(1, field_data_toUpdate_string);
						   			}
						   			else if(field_type[field_pos-1].contains("float"))
						   			{
						   				field_data_toUpdate_float=sc.nextFloat();
						   				pstmt.setFloat(1, field_data_toUpdate_float);
						   			}
						   		}
						   		pstmt.executeUpdate();
						   		System.out.println("Record Updated");
						   	}
					   	   else
					   		   System.out.println("Table not found");
						   		
						   		
						   break;
					   case 4:
							   System.out.println("Enter the name of the table:");
						   	   table_name=sc.next();
						   	   
						   	   
						   	   if(jdbc_test.checkTheExistenceOfTable(table_name))
						   	   {
						   		   System.out.println("Table found!!");
										// Create a result set
									   
									   stmt = conn.createStatement();
									    
									   ResultSet results = stmt.executeQuery("SELECT * FROM "+table_name);
									    
									    
									   // Get resultset metadata
									    
									   ResultSetMetaData metadata = results.getMetaData();
									    
									   //int columnCount = metadata.getColumnCount();
									    
									    //getting only primary key of the table
									     String columnName = metadata.getColumnName(1);
									     String columnType=metadata.getColumnTypeName(1);
									     System.out.println(columnType);
									     int delete_key_int;
									     String delete_key_string;
									     Float delete_key_float;
									     //Executing delete query
									     String delete_query="DELETE FROM "+table_name+" WHERE "+columnName+"=?;";
									     pstmt=conn.prepareStatement(delete_query);
									     
									     if(columnType.contains("INT"))
									     {
									    	 System.out.println("Enter "+columnName+" for which u want to delete a record");
									    	 delete_key_int=sc.nextInt();
									    	 pstmt.setInt(1,delete_key_int);
									     }
									     else if(columnType.contains("VARCHAR"))
									     {
									    	 System.out.println("Enter "+columnName+" for which u want to delete a record");
									    	 delete_key_string=sc.next();
									    	 pstmt.setString(1,delete_key_string);
									     }
									     else if(columnType.contains("FLOAT"))
									     {
									    	 System.out.println("Enter "+columnName+" for which u want to delete a record");
									    	 delete_key_float=sc.nextFloat();
									    	 pstmt.setFloat(1,delete_key_float);
									     }
									     if(pstmt.executeUpdate()==1)
									     System.out.println("Record deleted successfully");
									     else
									    	 System.out.println("Record does not exist!!");
		     
						    
					   	   		}
						   	 else
						   		   System.out.println("Table not found");
						   		
						   break;
					   case 5:
						   System.out.println("Enter the name of the table:");
					   	   table_name=sc.next();
					   	   
					   	   
					   	   if(jdbc_test.checkTheExistenceOfTable(table_name))
					   	   {
					   		   System.out.println("Table found!!");
					   		stmt=conn.createStatement();
					   		check_query="DESCRIBE "+table_name+";";
					   		ResultSet rs1=stmt.executeQuery(check_query);	   		
					   		i=0;
					   		while(rs1.next())
					   		{
					         //Retrieve by column name
					         field_name[i] = rs1.getString(1);
					         field_type[i] = rs1.getString(2);
					         i++;
					   		}
						   	  j=0;
				   			   while(j<i)
				   			   {
				   				   System.out.print(field_name[j]+"\t");
				   				   j++;
				   			   }
				   			   System.out.println("");
					   		   Statement stmt1=conn.createStatement();
					   		   String retrieval_query="SELECT * FROM "+table_name+";";
					   		   ResultSet rs=stmt1.executeQuery(retrieval_query);
					   		   while(rs.next())
					   		   {
					   			   j=0;
					   			   while(j<i)
					   			   {
					   				if(field_type[j].contains("int"))
					   					System.out.print(rs.getInt(field_name[j])+"\t");
							   		else if(field_type[j].contains("varchar"))
							   			System.out.print(rs.getString(field_name[j])+"\t");
							   		else if(field_type[j].contains("float"))
							   			System.out.print(rs.getFloat(field_name[j])+"\t");
					   				j++;   
					   			   }
					   			   System.out.println("");
					   		   }
					   	   }
					   	   else
					   		   System.out.println("Table not found");
					   		
					break;
					case 6:
						 System.out.println("Enter the name of the table:");
					   	   table_name=sc.next();
					   	   
					   	   
					   	   if(jdbc_test.checkTheExistenceOfTable(table_name))
					   	   {
					   		   System.out.println("Table found!!");
					   		// Create a result set
							   
							   stmt = conn.createStatement();
							    
							   ResultSet results = stmt.executeQuery("SELECT * FROM "+table_name);
							    
					   		 // Get resultset metadata
							    
							   ResultSetMetaData metadata = results.getMetaData();
							    
							   int columnCount = metadata.getColumnCount();
							    
							    //getting only primary key of the table
							     String columnName = metadata.getColumnName(1);
							     String columnType=metadata.getColumnTypeName(1);
							     System.out.println(columnType);
							     int search_key_int;
							     String search_key_string;
							     Float search_key_float;
							     
							     //Executing delete query
							     String search_query="SELECT * FROM "+table_name+" WHERE "+columnName+"=?";
							     pstmt=conn.prepareStatement(search_query);
							     
							     
							     if(columnType.contains("INT"))
							     {
							    	 System.out.println("Enter "+columnName+" for which u want to search a record");
							    	 search_key_int=sc.nextInt();
							    	 pstmt.setInt(1,search_key_int);
							    	
							     }
							     else if(columnType.contains("VARCHAR"))
							     {
							    	 System.out.println("Enter "+columnName+" for which u want to search a record");
							    	 search_key_string=sc.next();
							    	 pstmt.setString(1,search_key_string);
							    	 
							     }
							     else if(columnType.contains("FLOAT"))
							     {
							    	 System.out.println("Enter "+columnName+" for which u want to search a record");
							    	 search_key_float=sc.nextFloat();
							    	 pstmt.setFloat(1,search_key_float);
							    	 
							     }
							     System.out.println("Your Query:"+search_query);
							     
							     
							     ResultSet search_resultset=pstmt.executeQuery();
							     if(search_resultset.next())
							     {
							    	 System.out.println("Record found..");
							    	 do
							    	 {
							    		 j=0;
							    		 while(j<columnCount)
							    		 {
							    			 System.out.print(metadata.getColumnName(j+1)+":");
							    			 field_type[j]=metadata.getColumnTypeName(j+1);
							    				if(field_type[j].contains("INT"))
								   					System.out.print(search_resultset.getInt(j+1)+"\t");
										   		else if(field_type[j].contains("VARCHAR"))
										   			System.out.print(search_resultset.getString(j+1)+"\t");
										   		else if(field_type[j].contains("FLOAT"))
										   			System.out.print(search_resultset.getFloat(j+1)+"\t");
								   				j++;   
								   		 }
								   			   System.out.println("");
							    		 
							    	 }while(search_resultset.next());
							     }
							    	
							     else
							     {
							    	 System.out.println("Record not found!!");
							     }
							    	 
							   }
					   	   else
					   		   	System.out.println("Table not found");
					break;
					case 0:
					break;
					default:
						System.out.println("Invalid choice!!");
				   }
			   }while(choice!=0);
	   }
	   catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }finally{
		      //finally block used to close resources
		      try{
		         if(stmt!=null)
		            stmt.close();
		      }catch(SQLException se2){
		      }// nothing we can do
		      try{
		         if(conn!=null)
		            conn.close();
		      }catch(SQLException se){
		         se.printStackTrace();
		      }
		      try{
			      if(pstmt!=null)
			            pstmt.close();
			      }catch(SQLException se3){
			         se3.printStackTrace();
			      }
		   }///end finally try
	   
	   
	   }//end main
}

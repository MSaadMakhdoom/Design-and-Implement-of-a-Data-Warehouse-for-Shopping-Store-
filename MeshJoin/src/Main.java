
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Scanner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class Main 
{
	
	
	class Node 
	{
		String data;
		Node prev;
		Node next;

		public Node(String data) 
		{
			this.data = data;
		}
	}

	private Node head;

	private static String url;
	private static String SchemaName;
	private static String username;
	private static String password;
	private static Connection conn;
	static String  querytransaction ="SELECT * FROM metro_db.transactions;";
	
	static String querymaster="SELECT * FROM metro_db.masterdata;";
	
	static String insert_product_dw="INSERT IGNORE INTO data_warehouse_metro.product VALUES (?, ?)";
	
	
	static String insert_customer_dw="INSERT IGNORE INTO data_warehouse_metro.customer VALUES (?, ?)";
	
	static String insert_store_dw="INSERT IGNORE INTO data_warehouse_metro.store VALUES (?, ?)";
	
	static String insert_supplier_dw="INSERT IGNORE INTO data_warehouse_metro.supplier VALUES (?, ?)";
	
	static String insert_sale_dw="INSERT IGNORE INTO data_warehouse_metro.sales VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	
	private static Integer Transaction_Count_Read;

	private static Statement Query_TransactionData;

	private static Statement Query_MasterData;

	private static ResultSet Transaction;

	private static ResultSet MasterData_Result;

	private static Statement timeIDStatement;
	
	
	
	
	
	public static void main(String[] args) throws SQLException
	 {
		Scanner in = new Scanner(System.in);
		System.out.print("Enter data base Schema name");
		 SchemaName = in.next();
	     
		System.out.print("Enter MySQL user name");
		username = in.next();
		
		System.out.print("Enter MySQL Password");
		password  = in.next();
		
		url = "jdbc:mysql://localhost:3306/"+SchemaName;
		
		//username = "root";
		//password = "msaad";
		
		
		Sigin();
		System.out.println("Mesh Join Working");

		MeshJoin();
		
		System.out.println("Complete Program  ");
		System.out.println("Run the OLAP Query  ");
		
		close();
	}
	
	
	
	
	
	
	public static void Sigin() throws SQLException 
	{
		
		 try
		   {
		       
		
		
	
		conn = DriverManager.getConnection(url, username, password);

		Transaction_Count_Read = 1;
		
	
	    Query_TransactionData = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		
		Query_MasterData = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		Transaction = Query_TransactionData.executeQuery(querytransaction);
		
		MasterData_Result = Query_MasterData.executeQuery(querymaster);
		
		timeIDStatement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
	        }
			
		   catch (SQLException e) 
			{
			System.out.println("Error while connecting to the database");
			 e.printStackTrace();
			
			}
		loadDates();
	}
	
	
	public static void MeshJoin() throws SQLException 
	{
		HashTableQueue hashTableQueue = new HashTableQueue(1000);
		
		ArrayList<Transaction> TransactionStream_Buffer;
		
		ArrayList<MasterData> Masterdata_Buffer;
		
		int count = 0;
		
		while (true) 
		{
			
			if (endOfTransactions() && hashTableQueue.isEmpty())
			{
				break;
			}
			
			
			TransactionStream_Buffer = getTransactions(hashTableQueue.getCapacity());
			
			hashTableQueue.addTransactions(TransactionStream_Buffer);
			
			
			Masterdata_Buffer = getMasterData(hashTableQueue.getOldestEntry());
			
			
			for (MasterData masterDataTuple : Masterdata_Buffer) 
			{
				ArrayList<HashTableQueue.Value> joined = hashTableQueue.join(masterDataTuple.PRODUCT_ID);
				
				
				for (HashTableQueue.Value tuple : joined) 
				{
					tuple.transaction.addAttributes(masterDataTuple);
					
					shipToDW(tuple.transaction);					
					if (count<50) 
					{
						System.out.println((count+1)+". "+tuple.transaction);
						count++;
					}
				}
				hashTableQueue.discard(masterDataTuple.PRODUCT_ID);
			}
		}
	}
	private static void loadDates() throws SQLException 
	{
		PreparedStatement stmt = conn.prepareStatement("INSERT IGNORE INTO data_warehouse_metro.time VALUES (?, ?, ?, ?, ?, ?,?,?)");
		
		int time_id = 1;
	
		int quarter = -1;
		int Count=1;
		
		for (LocalDate date = LocalDate.parse("2016-01-01"); date.isBefore(LocalDate.parse("2017-01-01")); 
		date = date.plusDays(1))
		{
			
		
			time_id=time_id+1;
			stmt.setInt(1,time_id);
			
			
			
			stmt.setInt(2, date.getDayOfMonth());
			
			
			
			stmt.setString(3, date.getDayOfWeek().toString());
			
			
			
			
			stmt.setInt(4, date.getMonthValue());
			
			if (date.getMonthValue() >= 1 && date.getMonthValue() <= 3)
			{
				quarter = 1;
			}
			else if (date.getMonthValue() >= 4 && date.getMonthValue() <= 6)
			{
				quarter = 2;
			}
			else if (date.getMonthValue() >= 7 && date.getMonthValue() <= 9)
			{
				quarter = 3;
			}
			else if (date.getMonthValue() >= 10 && date.getMonthValue() <= 12)
			{
				quarter = 4;
			}
			
			
			stmt.setInt(5, quarter);
			
			stmt.setInt(6, date.getYear());
			
			
			if(date.getDayOfWeek().toString().equals("SUNDAY"))
			{
			 stmt.setInt(7, Count);
			 Count++;
			}
			else
			{
				stmt.setInt(7, Count);
			}
			if (date.getMonthValue() > 6)
			{
				stmt.setInt(8, 2);
			}
			else 
			{
				stmt.setInt(8, 1);
			}
			
			

			stmt.executeUpdate();
		}
	}

	private static int totalSize(ResultSet rs) throws SQLException 
	{
		int prev = rs.getRow();
		int totalSize = 0;
		
		if (rs != null) 
		{
			rs.last();
			totalSize = rs.getRow();
		}
		rs.absolute(prev);
		
		return totalSize;
	}

	private static boolean isEnd(ResultSet rs) throws SQLException 
	{
		return rs.getRow() == totalSize(rs);
	}

	
	public static ArrayList<Transaction> getTransactions(int rows) throws SQLException 
	{
		ArrayList<Transaction> transactionsToRet = new ArrayList<>(rows);
		
		for (int i = Transaction_Count_Read; (i < Transaction_Count_Read + rows) && !isEnd(Transaction); i++) 
		{
			Transaction.absolute(i);
			
			transactionsToRet.add(new Transaction(Transaction.getString("TRANSACTION_ID"),
					Transaction.getString("PRODUCT_ID"),
					Transaction.getString("CUSTOMER_ID"),
				    Transaction.getString("CUSTOMER_NAME"),
				    Transaction.getString("STORE_ID"),
					Transaction.getString("STORE_NAME"),Transaction.getDate("T_DATE"),
					Transaction.getInt("QUANTITY")));
		}
		Transaction_Count_Read += rows;
		return transactionsToRet;
	}

	public static ArrayList<MasterData> getMasterData(String PRODUCT_ID) throws SQLException {
		ArrayList<MasterData> masterDataToRet = new ArrayList<>(10);
		for (int i = 1; i <= totalSize(MasterData_Result); i++) 
		{
			MasterData_Result.absolute(i);
			if (MasterData_Result.getString("PRODUCT_ID").equals(PRODUCT_ID)) 
			{
				for (int j = i; j < i + 10 && j <= totalSize(MasterData_Result); j++) 
				{
					MasterData_Result.absolute(j);
					masterDataToRet.add(new MasterData(MasterData_Result.getString("PRODUCT_ID"),
							MasterData_Result.getString("PRODUCT_NAME"),
							MasterData_Result.getString("SUPPLIER_ID"),
							MasterData_Result.getString("SUPPLIER_NAME"),
							MasterData_Result.getFloat("PRICE")));
				}
				break;
			}
		}
		return masterDataToRet;
	}

	public static boolean endOfTransactions() throws SQLException 
	{
		return Transaction.getRow() == totalSize(Transaction);
	}

	public static void shipToDW(Transaction transaction) throws SQLException 
	{
		
		
		
		PreparedStatement stmt = conn.prepareStatement(insert_product_dw);
		stmt.setString(1, transaction.PRODUCT_ID);
		stmt.setString(2, transaction.PRODUCT_NAME);
		stmt.executeUpdate();
		
		stmt = conn.prepareStatement(insert_customer_dw);
		stmt.setString(1, transaction.CUSTOMER_ID);
		stmt.setString(2, transaction.CUSTOMER_NAME);
		stmt.executeUpdate();
	
		stmt = conn.prepareStatement(insert_store_dw);
		stmt.setString(1, transaction.STORE_ID);
		stmt.setString(2, transaction.STORE_NAME);
		stmt.executeUpdate();
		
		stmt = conn.prepareStatement(insert_supplier_dw);
		stmt.setString(1, transaction.SUPPLIER_ID);
		stmt.setString(2, transaction.SUPPLIER_NAME);
		stmt.executeUpdate();
		
		LocalDate localDate = transaction.T_DATE.toLocalDate();
		ResultSet time_rs = timeIDStatement.executeQuery("SELECT time.TIME_ID FROM data_warehouse_metro.time where time.DAY_OF_MONTH = "+ localDate.getDayOfMonth() + " and time.MONTH = " + localDate.getMonthValue()+ " and time.YEAR = " + localDate.getYear() + ";");
		time_rs.next();
		int time_id = time_rs.getInt(1);
	
		stmt = conn.prepareStatement(insert_sale_dw);
		stmt.setString(1, transaction.TRANSACTION_ID);
		stmt.setString(2, transaction.CUSTOMER_ID);
		stmt.setString(3, transaction.STORE_ID);
		stmt.setString(4, transaction.PRODUCT_ID);
		stmt.setString(5, transaction.SUPPLIER_ID);
		stmt.setInt(6, time_id);
		stmt.setInt(7, transaction.QUANTITY);
		stmt.setFloat(8, transaction.TOTAL_SALE);
		stmt.executeUpdate();
	}

	

	
		public Node getNode(String PRODUCT_ID) 
		{
			if (this.head == null) 
			{
				this.head = new Node(PRODUCT_ID);
				this.head.next = null;
				this.head.prev = null;
				return head;
			}

			Node iteratorNode = head;
			
			while (true) 
			{
				if (iteratorNode.data.equals(PRODUCT_ID))
				{
					return iteratorNode;
				}
				if (iteratorNode.next != null)
				{
					iteratorNode = iteratorNode.next;
				}
				else
				{
					break;
				}
			}
			Node newNode = new Node(PRODUCT_ID);
			newNode.next = null;
			newNode.prev = iteratorNode;
			iteratorNode.next = newNode;
			
			return newNode;
		}

		public int totalSize() 
		{
			int res = 0;
			Node node = this.head;
			while (node != null) 
			{
				res++;
				node = node.next;
			}
			return res;
		}



		public String getHeadData() 
		{
			if (this.head != null)
				return head.data;
			return null;
		}

		public void deleteNode(Node toDelete) 
		{
			if (this.head == null || toDelete == null)
				return;
			if (toDelete == this.head)
				this.head = toDelete.next;
			if (toDelete.next != null)
				toDelete.next.prev = toDelete.prev;
			if (toDelete.prev != null)
				toDelete.prev.next = toDelete.next;
			toDelete = null;
			return;
		}

		
		public static void close() throws SQLException 
		{
			Transaction.close();
			MasterData_Result.close();
			Query_MasterData.close();
			Query_TransactionData.close();
			conn.close();
		}


	

}

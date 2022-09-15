import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class Transaction 
{
	String TRANSACTION_ID;
	String PRODUCT_ID;
	String CUSTOMER_ID;
	String CUSTOMER_NAME;
	String STORE_ID;
	String STORE_NAME;
	Date T_DATE;
	Integer QUANTITY;
	
	String PRODUCT_NAME;
	String SUPPLIER_ID;
	String SUPPLIER_NAME;
	Float TOTAL_SALE;

	
	public Transaction(String TRANSACTION_ID, String PRODUCT_ID, String CUSTOMER_ID, String CUSTOMER_NAME,String STORE_ID, String STORE_NAME, Date t_DATE, Integer QUANTITY)
	{
		this.TRANSACTION_ID = TRANSACTION_ID;
		this.PRODUCT_ID = PRODUCT_ID;
		this.CUSTOMER_ID = CUSTOMER_ID;
		this.CUSTOMER_NAME = CUSTOMER_NAME;
		this.STORE_ID = STORE_ID;
		this.STORE_NAME = STORE_NAME;
		this.T_DATE = t_DATE;
		this.QUANTITY = QUANTITY;
	}
	
	@Override
	public String toString() 
	{
		return "TRANSACTION_ID=" + TRANSACTION_ID + ", PRODUCT_ID=" + PRODUCT_ID + ", CUSTOMER_ID=" + CUSTOMER_ID+ ", CUSTOMER_NAME=" + CUSTOMER_NAME + ", STORE_ID=" + STORE_ID + ", STORE_NAME=" + STORE_NAME+ ", T_DATE=" + T_DATE + ", QUANTITY=" + QUANTITY + ", PRODUCT_NAME=" + PRODUCT_NAME + ", SUPPLIER_ID="+ SUPPLIER_ID + ", SUPPLIER_NAME=" + SUPPLIER_NAME + ", TOTAL_SALE=" + TOTAL_SALE;
	}


	public void addAttributes(MasterData masterData) 
	{
		this.PRODUCT_NAME = masterData.PRODUCT_NAME;
		this.SUPPLIER_ID = masterData.SUPPLIER_ID;
		this.SUPPLIER_NAME = masterData.SUPPLIER_NAME;
		this.TOTAL_SALE = this.QUANTITY * masterData.PRICE;
	}

	

	
		
}

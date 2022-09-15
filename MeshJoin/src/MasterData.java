

public class MasterData 
{
	String PRODUCT_ID;
	String PRODUCT_NAME;
	String SUPPLIER_ID;
	String SUPPLIER_NAME;
	Float PRICE;

	public MasterData(String PRODUCT_ID, String PRODUCT_NAME, String SUPPLIER_ID, String SUPPLIER_NAME, Float PRICE) 
	{
		this.PRODUCT_ID = PRODUCT_ID;
		this.PRODUCT_NAME = PRODUCT_NAME;
		this.SUPPLIER_ID = SUPPLIER_ID;
		this.SUPPLIER_NAME = SUPPLIER_NAME;
		this.PRICE = PRICE;
	}

	void Set_Product_id(String P)
	{
		 this.PRODUCT_ID=P;
		
	}
	
	void Set_Product_Name(String Pname)
	{
		
		this.PRODUCT_NAME=Pname;
		
	}
	
	void Set_SUPPLIER_ID(String S)
	{
		 this.SUPPLIER_ID=S;
		
	}
	
	void Set_SUPPLIER_NAME(String name)
	{
		this.SUPPLIER_NAME=name;
		
	}
	void Set_PRICE(Float p)
	{
		 this.PRICE=p;
		
	}

	
	
	
	
	
	
	
	String get_Product_id()
	{
		return this.PRODUCT_ID;
		
	}
	
	String get_Product_Name()
	{
		return this.PRODUCT_NAME;
		
	}
	String get_SUPPLIER_ID()
	{
		return this.SUPPLIER_ID;
		
	}
	
	String get_SUPPLIER_NAME()
	{
		return this.SUPPLIER_NAME;
		
	}
	Float get_PRICE()
	{
		return this.PRICE;
		
	}
	
 }











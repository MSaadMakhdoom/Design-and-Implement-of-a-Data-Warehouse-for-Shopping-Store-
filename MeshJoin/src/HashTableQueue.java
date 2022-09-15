import java.util.ArrayList;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

public class HashTableQueue 
{

	class Value 
	{
		Main.Node queueNode;
		Transaction transaction;

		public Value(Main.Node queueNode, Transaction transaction) 
		{
			this.queueNode = queueNode;
			
			this.transaction = transaction;
		}
	}

	private Integer size;

	private MultiValuedMap<String, Value> hashTable;

	private Main queue;

	public HashTableQueue(Integer size) 
	{
		this.size = size;
		
		this.hashTable = new ArrayListValuedHashMap<String, Value>(this.size);
		
		this.queue = new Main();
	}

	public void addTransactions(ArrayList<Transaction> transactions) 
	{
		for (Transaction transaction : transactions) 
		{
			
			//System.out.println("hash table: "+transaction.PRODUCT_ID+"  "+ transaction.PRODUCT_NAME+"   "+transaction.TRANSACTION_ID+"   "+transaction.CUSTOMER_ID +" "+transaction.CUSTOMER_NAME +"    "+  transaction.STORE_ID +" "+ transaction.STORE_NAME +" "+ transaction.QUANTITY +" "+ transaction.TOTAL_SALE +" "+ transaction.T_DATE);
		
			
			this.hashTable.put(transaction.PRODUCT_ID,new Value(this.queue.getNode(transaction.PRODUCT_ID), transaction));
		}
	}

	
	
	public String getOldestEntry() 
	{
		return this.queue.getHeadData();
	}

	
	
	public int getCapacity() 
	{
		return 100 - this.hashTable.size();
	}


	
	public ArrayList<Value> join(String PRODUCT_ID) 
	{
		return new ArrayList<Value>(hashTable.get(PRODUCT_ID));
	}


	
	public void discard(String PRODUCT_ID) 
	{
		if (hashTable.containsKey(PRODUCT_ID)) 
		{
			this.queue.deleteNode(new ArrayList<Value>(hashTable.get(PRODUCT_ID)).get(0).queueNode);
			this.hashTable.remove(PRODUCT_ID);
		}
	}

	public boolean isEmpty() 
	{
		return this.hashTable.size() == 0;
	}
	
	


}

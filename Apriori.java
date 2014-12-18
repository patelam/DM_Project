package cmpt741;

import java.util.*;
import java.util.Map.Entry;

/* This file is copyright (c) 2008-2012 Philippe Fournier-Viger
* 
* This file is part of the SPMF DATA MINING SOFTWARE
* (http://www.philippe-fournier-viger.com/spmf).
* 
* SPMF is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* SPMF. If not, see <http://www.gnu.org/licenses/>.
* 
* Modified by Ankit Patel and Margaret Dulat for CMPT 741 Project
*/

/*************************************************************************************
 * This is an optimized implementation of the Apriori algorithm that uses binary search to
 * check if subsets of a candidate are frequent and other optimizations.
 * ***********************************************************************************/
public class Apriori 
{
	double minsupport;
	
	public Apriori(double minsupport) 
	{
		this.minsupport = minsupport;
	}
	
	public Apriori() 
	{	
	}

	public Itemsets runApriori(String baskets) 
	{
		List<int[]> database =  new ArrayList<int[]>();;
		int databaseSize=0;
		Itemsets patterns = new Itemsets();
		Map<Integer, Integer> mapItemCount = new HashMap<Integer, Integer>(); // to count the support of each item
		
		String lines[] = baskets.split("\r?&|\r");
		
		for (String line : lines)
		{ 
			String[] lineSplited = line.split(" ");
			int transaction[] = new int[lineSplited.length];
			for (int i=0; i< lineSplited.length; i++) 
			{
                if(!"".equalsIgnoreCase(lineSplited[i]))
                {
				    Integer item = Integer.parseInt(lineSplited[i]);
				    transaction[i] =item;
				    Integer count = mapItemCount.get(item);
				    
				    if (count == null) 
				    {
					    mapItemCount.put(item, 1);
				    } else 
				    {
					    mapItemCount.put(item, ++count);
				    }
                }
			}
			
			Arrays.sort(transaction);
			database.add(transaction);
			databaseSize++;
		}

		this.minsupport = (int) Math.ceil(minsupport * databaseSize);
		
		List<Integer> frequent1 = new ArrayList<Integer>();
		for(Entry<Integer, Integer> entry : mapItemCount.entrySet())
		{
			if(entry.getValue() >= minsupport)
			{
				frequent1.add(entry.getKey());
				Itemset itemset = new Itemset(entry.getKey());
				itemset.setSupport(entry.getValue());
				patterns.addItemset(itemset, 1);
			}
		}
		
		mapItemCount = null;
		
		/*Collections.sort(frequent1, new Comparator<Integer>() 
		{
			public int compare(Integer o1, Integer o2) 
			{
				return (o1-o2);
			}
		});
		*/
		Arrays.sort(frequent1.toArray(new Integer[frequent1.size()]));
		if(frequent1.size() == 0)
		{
			return patterns; 
		}
		
		List<Itemset> level = null;
		int k = 2;
		do {
			List<Itemset> candidatesK;
			
			if(k ==2)
			{
				candidatesK = generateCandidate2(frequent1);
			}
			else
			{
				candidatesK = generateCandidateSizeK(level);
			}
			
			for(int[] transaction: database)
			{
				if(transaction.length < k) 
				{
					continue;
				}
				
	 loopCand:	for(Itemset candidate : candidatesK)
	 			{
					int pos = 0;
					for(int item: transaction)
					{
						if(item == candidate.itemset[pos])
						{
							pos++;
							if(pos == candidate.itemset.length)
							{
								candidate.support++;
								continue loopCand;
							}
					  	}
						else if(item > candidate.itemset[pos])
						{
							continue loopCand;
						}
					}
				}
			}
           
			level = new ArrayList<Itemset>();
			for (Itemset candidate : candidatesK)
			{
				if (candidate.getSupport() >= minsupport)
				{
					Arrays.sort(candidate.itemset);
					level.add(candidate);
					patterns.addItemset(candidate, candidate.size());
					//itemsetCount++;
				}
			}
	
			k++;
		} while(level.isEmpty() == false);
		
		return patterns;
	}
	
	private List<Itemset> generateCandidate2(List<Integer> frequent1) 
	{
		List<Itemset> candidates = new ArrayList<Itemset>();
		
		for (int i = 0; i < frequent1.size(); i++) 
		{
			Integer item_1 = frequent1.get(i);
			for (int j = i + 1; j < frequent1.size(); j++) 
			{
				Integer item_2 = frequent1.get(j);
				candidates.add(new Itemset(new int []{item_1, item_2}));
			}
		}
		return candidates;
	}
	
	private List<Itemset> generateCandidateSizeK(List<Itemset> levelK_1)
	{
		List<Itemset> candidates = new ArrayList<Itemset>();

		loop1: for (int i = 0; i < levelK_1.size(); i++) 
		{
			int[] itemset1 = levelK_1.get(i).itemset;
			loop2: for (int j = i + 1; j < levelK_1.size(); j++) 
			{
				int[] itemset2 = levelK_1.get(j).itemset;

				for (int k = 0; k < itemset1.length; k++) 
				{
					if (k == itemset1.length - 1) 
					{
						if (itemset1[k] >= itemset2[k]) 
						{
							continue loop1;
						}
					}
					
					else if (itemset1[k] < itemset2[k])
					{
						continue loop2;
					} 
					else if (itemset1[k] > itemset2[k])
					{
						continue loop1; 
					}
				}

				// Create a new candidate by combining itemset1 and itemset2
				int newItemset[] = new int[itemset1.length+1];
				System.arraycopy(itemset1, 0, newItemset, 0, itemset1.length);
				newItemset[itemset1.length] = itemset2[itemset2.length -1];

			    if (allSubsetsOfSizeK_1AreFrequent(newItemset, levelK_1))
			    {
					candidates.add(new Itemset(newItemset));
				}
			}
		}
		return candidates; 
	}
	
	private boolean allSubsetsOfSizeK_1AreFrequent(int[] candidate, List<Itemset> levelK_1) 
	{
		for(int posRemoved=0; posRemoved< candidate.length; posRemoved++)
		{
	        int first = 0;
	        int last = levelK_1.size() - 1;
	       
	        // variable to remember if we found the subset
	        boolean found = false;
	        // the binary search
	        while( first <= last )
	        {
	        	int middle = ( first + last ) >>1 ; // >>1 means to divide by 2

	        	int comparison =sameAs(levelK_1.get(middle).getItems(), candidate, posRemoved);
	            if(comparison < 0 )
	            {
	            	first = middle + 1;  //  the itemset compared is larger than the subset according to the lexical order
	            }
	            else if(comparison  > 0 )
	            {
	            	last = middle - 1; //  the itemset compared is smaller than the subset  is smaller according to the lexical order
	            }
	            else
	            {
	            	found = true; //  we have found it so we stop
	                break;
	            }
	        }

			if(found == false)  
				return false;
		}
		return true;
	}
	
	public static int sameAs(int [] itemset1, int [] itemsets2, int posRemoved) 
	{
		int j=0;
		for(int i=0; i<itemset1.length; i++)
		{

			if(j == posRemoved)
			{
				j++;
			}

			if(itemset1[i] == itemsets2[j])
			{
				j++;
		    }
			else if (itemset1[i] > itemsets2[j])
			{
				return 1;
			}
			else
			{
				return -1;
			}
		}
		return 0;
	}
}

/***********************************************************************************
 * This class represents a set of itemsets, where an itemset is an array of strings 
 * with an associated support count. 
 **********************************************************************************/
class Itemsets
{
	private int totalItemsets = 0;
	private final List<List<Itemset>> levels = new ArrayList<List<Itemset>>(); 
	
	public Itemsets() 
	{
		levels.add(new ArrayList<Itemset>());
	}
	
	public List<List<Itemset>> getLevel()
	{
		return this.levels;
	}
	
	public int getTotalItemsets()
	{
		return totalItemsets;
	}
	
	public void addItemset(Itemset itemset, int k) 
	{
		while (levels.size() <= k) 
		{
			levels.add(new ArrayList<Itemset>());
		}
		levels.get(k).add(itemset);
		totalItemsets ++;
	}
}

/************************************************************************************
 * This class represents an itemset (a set of items) implemented as an array of integers with
 * a variable to store the support count of the itemset.
 ***********************************************************************************/
class Itemset
{
	public int[] itemset;
	double support = 0; 
	
	public Itemset()
	{
		itemset = new int[]{};
	}
	
	public Itemset(Integer item)
	{
		itemset = new int[]{item};
	}
	
	public double getSupport()
	{
		return support;
	}
	
	public Itemset(int [] items)
	{
		this.itemset = items;
	}
	
	public int size() 
	{
		return itemset.length;
	}
	
	public int[] getItems() 
	{
		return itemset;
	}
	
	public void setSupport(double support)
	{
		this.support=support;
	}
}

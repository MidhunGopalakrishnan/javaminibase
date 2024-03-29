package iterator;


import heap.*;
import global.*;
import java.io.*;
import java.lang.*;

/**
 *some useful method when processing Tuple 
 */
public class TupleUtils
{
  
  /**
   * This function compares a tuple with another tuple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple.
   *@param    t2        another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.
   *@param    t2_fld_no the field numbers in the tuples to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,                              
   */
  public static int CompareTupleWithTuple(AttrType fldType,
					  Tuple  t1, int t1_fld_no,
					  Tuple  t2, int t2_fld_no)
    throws IOException,
	   UnknowAttrType,
	   TupleUtilsException
    {
      int   t1_i,  t2_i;
      float t1_r,  t2_r;
      String t1_s, t2_s;
      
      switch (fldType.attrType) 
	{
	case AttrType.attrInteger:                // Compare two integers.
	  try {
	    t1_i = t1.getIntFld(t1_fld_no);
	    t2_i = t2.getIntFld(t2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  if (t1_i == t2_i) return  0;
	  if (t1_i <  t2_i) return -1;
	  if (t1_i >  t2_i) return  1;
	  
	case AttrType.attrReal:                // Compare two floats
	  try {
	    t1_r = t1.getFloFld(t1_fld_no);
	    t2_r = t2.getFloFld(t2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  if (t1_r == t2_r) return  0;
	  if (t1_r <  t2_r) return -1;
	  if (t1_r >  t2_r) return  1;
	  
	case AttrType.attrString:                // Compare two strings
	  try {
	    t1_s = t1.getStrFld(t1_fld_no);
	    t2_s = t2.getStrFld(t2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  
	  // Now handle the special case that is posed by the max_values for strings...
	  if(t1_s.compareTo( t2_s)>0)return 1;
	  if (t1_s.compareTo( t2_s)<0)return -1;
	  return 0;
	default:
	  
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
    }
  
  
  
  /**
   * This function  compares  tuple1 with another tuple2 whose
   * field number is same as the tuple1
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple
   *@param    value     another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.  
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,  
   *@exception UnknowAttrType don't know the attribute type   
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class   
   */            
  public static int CompareTupleWithValue(AttrType fldType,
					  Tuple  t1, int t1_fld_no,
					  Tuple  value)
    throws IOException,
	   UnknowAttrType,
	   TupleUtilsException
    {
      return CompareTupleWithTuple(fldType, t1, t1_fld_no, value, t1_fld_no);
    }

	public static boolean dominates(Tuple t1, AttrType[] type1,
									Tuple t2,
									AttrType[] type2,
									short len_in,
									short[] str_sizes,
									int[] pref_list,
									int pref_list_length) throws IOException, FieldNumberOutOfBoundException {

		boolean chanceToDominate = false;
		int matchingAttributes = 0;
		for (int i = 0; i < pref_list_length; i++) {
			float firstValueFloat = 0f;
			float secondValueFloat = 0f;
			String firstString = null;
			String secondString = null;

			//get first preferred attribute value and compare
			if (type1[pref_list[i]].toString().equals("attrInteger")) {
				firstValueFloat = (float) t1.getIntFld(pref_list[i]+1);
			} else if (type1[pref_list[i]].toString().equals("attrReal")) {
				firstValueFloat = t1.getFloFld(pref_list[i]+1);
			} else if (type1[pref_list[i]].toString().equals("attrString")) {
				firstString = t1.getStrFld(pref_list[i]);
			}

			if (type2[pref_list[i]].toString().equals("attrInteger")) {
				secondValueFloat = (float) t2.getIntFld(pref_list[i]+1);
			} else if (type2[pref_list[i]].toString().equals("attrReal")) {
				secondValueFloat = t2.getFloFld(pref_list[i]+1);
			} else if (type2[pref_list[i]].toString().equals("attrString")) {
				secondString = t2.getStrFld(pref_list[i]);
			}
			// if attribute value is integer or float
			if(firstString == null && secondString == null) {
				if(firstValueFloat> secondValueFloat){
					chanceToDominate = true;
				}
				else if(firstValueFloat == secondValueFloat){
					chanceToDominate = true;
					matchingAttributes++;
				}
				else {
					return false; // exit for loop as there is not chance now the t1 tuple will dominate
				}
			}
			else if (firstString!= null && secondString!=null){
				if(firstString.compareTo(secondString)>0){
					chanceToDominate = true;
				}
				else if(firstString.compareTo(secondString)== 0){
					chanceToDominate = true;
					matchingAttributes++;
				}
				else {
					return false; // exit for loop as there is not chance now the t1 tuple will dominate
				}
			}
		}
		//if two tuples have same preference attribute values, return false
		if(matchingAttributes == pref_list_length) {
			chanceToDominate = false;
		}
		return chanceToDominate;
	}

	public static int CompareTupleWithTuplePref(Tuple t1, AttrType[] type1, Tuple t2,
												AttrType[] type2,
												short len_in,
												short[] str_sizes,
												int[] pref_list,
												int pref_list_length) throws IOException, FieldNumberOutOfBoundException {

		float t1Sum =0f;
		float t2Sum =0f;
		String firstTupleString = null;
		String secondTupleString =null;
		for (int i = 0; i < pref_list_length; i++) {
			if (type1[pref_list[i]].toString().equals("attrInteger")) {
				t1Sum+= (float) t1.getIntFld(pref_list[i]+1);
			} else if (type1[pref_list[i]].toString().equals("attrReal")) {
				t1Sum+= t1.getFloFld(pref_list[i]+1);
			} else if (type1[pref_list[i]].toString().equals("attrString")) {
				firstTupleString = t1.getStrFld(pref_list[i]);
			}
			if (type2[pref_list[i]].toString().equals("attrInteger")) {
				t2Sum+= (float) t2.getIntFld(pref_list[i]+1);
			} else if (type2[pref_list[i]].toString().equals("attrReal")) {
				t2Sum+= t2.getFloFld(pref_list[i]+1);
			} else if (type2[pref_list[i]].toString().equals("attrString")) {
				secondTupleString = t2.getStrFld(pref_list[i]);
				t1Sum+= firstTupleString.compareTo(secondTupleString);
			}

		}
		if(t1Sum-t2Sum >0) {
			return 1;
		} else if(t1Sum-t2Sum <0){
			return -1;
		}
		return 0;
	}

	public static float computeTupleSumOfPrefAttrs(Tuple t1, AttrType[] type1,
												   short len_in,
												   short[] str_sizes,
												   int[] pref_list,
												   int pref_list_length) throws IOException, FieldNumberOutOfBoundException {
		float tupleSum =0;
		for (int i = 0; i < pref_list_length; i++) {
			if (type1[pref_list[i]].toString().equals("attrInteger")) {
				tupleSum+= (float) t1.getIntFld(pref_list[i]+1);
			} else if (type1[pref_list[i]].toString().equals("attrReal")) {
				tupleSum+= t1.getFloFld(pref_list[i]+1);
			} else if (type1[pref_list[i]].toString().equals("attrString")) {
				//TODO: currently no support for String. Add code as needed
			}
		}

		return tupleSum;
	}
  
  /**
   *This function Compares two Tuple inn all fields 
   * @param t1 the first tuple
   * @param t2 the secocnd tuple
   * @param //type[] the field types
   * @param len the field numbers
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */            
  
  public static boolean Equal(Tuple t1, Tuple t2, AttrType types[], int len)
    throws IOException,UnknowAttrType,TupleUtilsException
    {
      int i;
      
      for (i = 1; i <= len; i++)
	if (CompareTupleWithTuple(types[i-1], t1, i, t2, i) != 0)
	  return false;
      return true;
    }
  
  /**
   *get the string specified by the field number
   *@param tuple the tuple 
   *@param //fidno the field number
   *@return the content of the field number
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */
  public static String Value(Tuple  tuple, int fldno)
    throws IOException,
	   TupleUtilsException
    {
      String temp;
      try{
	temp = tuple.getStrFld(fldno);
      }catch (FieldNumberOutOfBoundException e){
	throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
      }
      return temp;
    }

    //$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
	/**
	 *set up a tuple in specified field from a tuple
	 *@param value the tuple to be set
	 *@param tuple the given tuple
	 *@param fld_nos the field number
	 *@param fldType the tuple attr type
	 *@exception UnknowAttrType don't know the attribute type
	 *@exception IOException some I/O fault
	 *@exception TupleUtilsException exception from this class
	 */
	public static void SetValue(Tuple value, Tuple  tuple, int[] fld_nos, AttrType[] fldType)
			throws IOException,
			UnknowAttrType,
			TupleUtilsException
	{
for(int i = 0; i < fld_nos.length; i++) {
	switch (fldType[i].attrType) {
		case AttrType.attrInteger:
			try {
				value.setIntFld(fld_nos[i]+1, tuple.getIntFld(fld_nos[i] +1));
			} catch (FieldNumberOutOfBoundException e) {
				throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
			}
			break;
		case AttrType.attrReal:
			try {
				value.setFloFld(fld_nos[i]+1, tuple.getFloFld(fld_nos[i]+1));
			} catch (FieldNumberOutOfBoundException e) {
				throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
			}
			break;
		case AttrType.attrString:
			try {
				value.setStrFld(fld_nos[i], tuple.getStrFld(fld_nos[i]));
			} catch (FieldNumberOutOfBoundException e) {
				throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
			}
			break;
		default:
			throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

	}
}

		return;
	}

  /**
   *set up a tuple in specified field from a tuple
   *@param value the tuple to be set 
   *@param tuple the given tuple
   *@param fld_no the field number
   *@param fldType the tuple attr type
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */  
  public static void SetValue(Tuple value, Tuple  tuple, int fld_no, AttrType fldType)
    throws IOException,
	   UnknowAttrType,
	   TupleUtilsException
    {
      
      switch (fldType.attrType)
	{
	case AttrType.attrInteger:
	  try {
	    value.setIntFld(fld_no, tuple.getIntFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  break;
	case AttrType.attrReal:
	  try {
	    value.setFloFld(fld_no, tuple.getFloFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  break;
	case AttrType.attrString:
	  try {
	    value.setStrFld(fld_no, tuple.getStrFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  break;
	default:
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
      
      return;
    }
  
  
  /**
   *set up the Jtuple's attrtype, string size,field number for using join
   *@param Jtuple  reference to an actual tuple  - no memory has been malloced
   *@param res_attrs  attributes type of result tuple
   *@param in1  array of the attributes of the tuple (ok)
   *@param len_in1  num of attributes of in1
   *@param in2  array of the attributes of the tuple (ok)
   *@param len_in2  num of attributes of in2
   *@param t1_str_sizes shows the length of the string fields in S
   *@param t2_str_sizes shows the length of the string fields in R
   *@param proj_list shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */
  public static short[] setup_op_tuple(Tuple Jtuple, AttrType[] res_attrs,
				       AttrType in1[], int len_in1, AttrType in2[], 
				       int len_in2, short t1_str_sizes[], 
				       short t2_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   TupleUtilsException
    {
      short [] sizesT1 = new short [len_in1];
      short [] sizesT2 = new short [len_in2];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesT1[i] = t1_str_sizes[count++];
      
      for (count = 0, i = 0; i < len_in2; i++)
	if (in2[i].attrType == AttrType.attrString)
	  sizesT2[i] = t2_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer)
	    res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  else if (proj_list[i].relation.key == RelSpec.innerRel)
	    res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
	}
      try {
	Jtuple.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new TupleUtilsException(e,"setHdr() failed");
      }
      return res_str_sizes;
    }
  
 
   /**
   *set up the Jtuple's attrtype, string size,field number for using project
   *@param Jtuple  reference to an actual tuple  - no memory has been malloced
   *@param res_attrs  attributes type of result tuple
   *@param in1  array of the attributes of the tuple (ok)
   *@param len_in1  num of attributes of in1
   *@param t1_str_sizes shows the length of the string fields in S
   *@param proj_list shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */

  public static short[] setup_op_tuple(Tuple Jtuple, AttrType res_attrs[],
				       AttrType in1[], int len_in1,
				       short t1_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   TupleUtilsException, 
	   InvalidRelation
    {
      short [] sizesT1 = new short [len_in1];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesT1[i] = t1_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer) 
            res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  
	  else throw new InvalidRelation("Invalid relation -innerRel");
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer
	      && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
	    n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++) {
	if (proj_list[i].relation.key ==RelSpec.outer
	    && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
	  res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
      }
     
      try {
	Jtuple.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new TupleUtilsException(e,"setHdr() failed");
      } 
      return res_str_sizes;
    }
}





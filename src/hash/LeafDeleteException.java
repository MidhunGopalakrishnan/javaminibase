package hash;
import chainexception.*;

public class LeafDeleteException extends ChainException 
{
  public LeafDeleteException() {super();}
  public LeafDeleteException(Exception e, String s) {super(e,s);}

}

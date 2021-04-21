package iterator;

import java.lang.*;
import chainexception.*;

public class GroupByException extends ChainException
{
    public GroupByException(String s) {super(null,s);}
    public GroupByException(Exception e, String s) {super(e,s);}
}

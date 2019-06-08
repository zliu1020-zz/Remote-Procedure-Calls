import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
    {
	try {
	    List<String> ret = new ArrayList<>();
        for(String pw: password){
	        String oneHash = BCrypt.hashpw(pw, BCrypt.gensalt(logRounds));
	        ret.add(oneHash);
        }
	    return ret;
	} catch (Exception e) {
	    throw new IllegalArgument(e.getMessage());
	}
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
    {
	try {
	    List<Boolean> ret = new ArrayList<>();
        for(int idx = 0; idx < password.size(); idx++){
	       String onePwd = password.get(idx);
	       String oneHash = hash.get(idx);
	       ret.add(BCrypt.checkpw(onePwd, oneHash));            
        }
	    return ret;
	} catch (Exception e) {
	    throw new IllegalArgument(e.getMessage());
	}
    }
}

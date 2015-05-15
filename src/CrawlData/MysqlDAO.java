package CrawlData;

import java.sql.PreparedStatement;

import com.mysql.jdbc.Connection;

public interface MysqlDAO {
	public boolean insertForTA(String[] record);
//	public boolean insertForQUOTE(String[] record);
	public void insertForTA(String[] record, PreparedStatement prepareStatement);
	public void insertForQUOTE(String[] record, PreparedStatement preparedStatement);

}

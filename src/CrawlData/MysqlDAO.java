package CrawlData;

import java.sql.PreparedStatement;

import com.mysql.jdbc.Connection;

public interface MysqlDAO {
	public boolean insert(String[] record);
	public void insert(String[] record, PreparedStatement prepareStatement);

}

package CrawlData;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;

public class MysqlDAOImpl implements MysqlDAO {

	public boolean insert(String[] record) {
		// TODO Auto-generated method stub
		Connection conn = MysqlDBUtil.getConnection();
		String sql = "insert into index_ta(Flag,TransactionTime,ContractId,TAIndex,Buy1Price,"
				+ "Buy2Price,Buy3Price,Buy4Price,Buy5Price,Buy1Num,Buy2Num,Buy3Num,Buy4Num,"
				+ "Buy5Num,Sell1Price,Sell2Price,Sell3Price,Sell4Price,Sell5Price,Sell1Num,"
				+ "Sell2Num,Sell3Num,Sell4Num,Sell5Num,m_dZJSJ,m_dJJSJ,m_dCJJJ,m_dZSP,m_dJSP,"
				+ "m_dJKP,m_nZCCL,m_nCCL,m_dZXJ,m_nCJSL,m_dCJJE,m_dZGBJ,m_dZDBJ,m_dZGJ,m_dZDJ,m_dZXSD,m_dJXSD) "
				+ "value(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try {
			PreparedStatement prepareStatement = conn.prepareStatement(sql);

			for (int i = 0; i < 3; i++) {
				prepareStatement.setString(i + 1, record[i]);
			}
			for (int i = 0; i < 6; i++) {
				prepareStatement.setFloat(i + 4,
						Float.parseFloat(record[i + 3]));
			}
			for (int i = 0; i < 5; i++) {
				prepareStatement
						.setInt(i + 10, Integer.parseInt(record[i + 9]));
			}
			for (int i = 0; i < 5; i++) {
				prepareStatement.setFloat(i + 15,
						Float.parseFloat(record[i + 14]));
			}
			for (int i = 0; i < 5; i++) {
				prepareStatement.setInt(i + 20,
						Integer.parseInt(record[i + 19]));
			}
			for (int i = 0; i < 6; i++) {
				prepareStatement.setFloat(i + 25,
						Float.parseFloat(record[i + 24]));
			}
			for (int i = 0; i < 2; i++) {
				prepareStatement.setInt(i + 31,
						Integer.parseInt(record[i + 30]));
			}
			prepareStatement.setFloat(33, Float.parseFloat(record[32]));
			prepareStatement.setInt(34, Integer.parseInt(record[33]));
			for (int i = 0; i < 7; i++) {
				prepareStatement.setFloat(i + 35,
						Float.parseFloat(record[i + 34]));
			}
			prepareStatement.executeUpdate();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public void insert(String[] record, PreparedStatement prepareStatement) {
		// TODO Auto-generated method stub

		for (int i = 0; i < 3; i++) {
			try {
				prepareStatement.setString(i + 1, record[i]);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 6; i++) {
			try {
				prepareStatement.setFloat(i + 4, Float.parseFloat(record[i + 3]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 5; i++) {
			try {
				prepareStatement.setInt(i + 10, Integer.parseInt(record[i + 9]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 5; i++) {
			try {
				prepareStatement.setFloat(i + 15, Float.parseFloat(record[i + 14]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 5; i++) {
			try {
				prepareStatement.setInt(i + 20, Integer.parseInt(record[i + 19]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 6; i++) {
			try {
				prepareStatement.setFloat(i + 25, Float.parseFloat(record[i + 24]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (int i = 0; i < 2; i++) {
			try {
				prepareStatement.setInt(i + 31, Integer.parseInt(record[i + 30]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			prepareStatement.setFloat(33, Float.parseFloat(record[32]));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			prepareStatement.setInt(34, Integer.parseInt(record[33]));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < 7; i++) {
			try {
				prepareStatement.setFloat(i + 35, Float.parseFloat(record[i + 34]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

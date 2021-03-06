package com.ftoul.bi.clean;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        // System.out.println(CleanMap.cleans.size());
    	Matcher matcher = Pattern.compile("([^=]+)=([^=]+)").matcher("fromFormat=yyyy-MM-dd HH:mm:ss");
    	boolean isValid = matcher.matches();
    	System.out.println(isValid);
    	System.out.println(matcher.group(1));
    	System.out.println(matcher.group(2));
    }
}

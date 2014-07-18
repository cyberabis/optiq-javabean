package io.thedal.optiq.javabean.utils;

import java.lang.reflect.Method;
import java.util.Date;

/**
 * Utility class to check eligible fields in a Java Bean.
 * 
 * @author Abishek Baskaran
 *
 */
public class JavaBeanInspector {

  /**
   * Given method, determines if the method is a getter for an eligible field.
   * 
   * @param method
   *          Java Method
   * @return boolean representing eligible or not.
   */
  public static boolean checkMethodEligiblity(Method method) {
    if ((method.getName().startsWith("get"))
        && ((method.getReturnType() == Integer.class)
            || (method.getReturnType() == String.class)
            || (method.getReturnType() == Float.class) || (method
            .getReturnType() == Date.class))) {
      return true;
    } else {
      return false;
    }
  }

}

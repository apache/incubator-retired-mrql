/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mrql.gen;

abstract public class Crypt {
    public static boolean encryptp = false;
    public static String crypt ( String s ) {
        byte[] b = s.getBytes();
        for (int i=0; i<b.length; i++)
        {   if (b[i]<45)
               b[i] = (byte) (b[i]+78);
            else b[i] = (byte) (b[i]-13);
            if (b[i]==34)
               b[i] = 123;
            else if (b[i]==92)
               b[i] = 124;
            else if (b[i]=='\n')
               b[i] = 125;
        };
        return new String(b);
    }
    public static String decrypt ( String s ) {
        byte[] b = s.getBytes();
        for (int i=0; i<b.length; i++)
        {   if (b[i]==123)
               b[i] = 47;
            else if (b[i]==124)
               b[i] = 105;
            else if (b[i]==125)
               b[i] = '\n';
            else if (b[i]>109)
               b[i] = (byte) (b[i]-78);
            else b[i] = (byte) (b[i]+13);
        };
        return new String(b);
    }
    public static String quotes ( String s ) { return "\"" + s + "\""; }
    public static String encrypt ( String s ) {
        if (encryptp)
            return "Crypt.decrypt(\"" + crypt(s) + "\")";
        else return quotes(s);
    }
}

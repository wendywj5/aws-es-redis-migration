### Clone from https://github.com/SaminOz/redis-random-data-generator/blob/master/generator.js
AWS linux installation:

  > curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
  > . ~/.nvm/nvm.sh
  > nvm install node
Install dependencies:

  > npm install lorem-ipsum@1.0.3
  > npm install uuid
  > npm install redis-stream
  > npm install wordwrap


 Basic Usage:                                                        
===================================================================  
  > `node generator.js <type> <qty> [<key_prefix>]`                  
                                                                                                                                                                
   `node generator.js hash 100 session`                              
   `1)...`                                                           
   `100) "session:ffab3b35-09c3-4fd7-9af1-4d323534065e"`             
                                            
                                                                     
-------------------------------------------------------------------  
 Types (others may be added in future versions i.e. geohash):        
===================================================================  
 * 'string'  uses SET to add a redis string value                    
 * 'list'    uses LPUSH to add a random number of values to a list   
 * 'set'     uses SADD to add a random number of values to a set     
 * 'sorted'  uses ZADD to add a random number of values and scores   
   to a sorted set.                                                  
 * 'hash'    uses HMSET to add a random number of values to a hash   
*------------------------------------------------------------------* 

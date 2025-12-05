import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config'; // <--- IMPORT THIS
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { RidesModule } from './rides/rides.module';

@Module({
  imports: [
    // This line loads the .env file globally so every service can use it
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    RidesModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}

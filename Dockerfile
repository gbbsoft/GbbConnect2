#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["GbbConnect2Console/GbbConnect2Console.csproj", "GbbConnect2Console/"]
RUN dotnet restore "GbbConnect2Console/GbbConnect2Console.csproj"
COPY . .
WORKDIR "/src/GbbConnect2Console"
RUN dotnet build "GbbConnect2Console.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "GbbConnect2Console.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "GbbConnect2Console.dll"]
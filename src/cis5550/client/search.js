async function search(event, page = 1) {
  event.preventDefault();

  // Clear any existing search results from the page
  const searchResults = document.getElementById("search-results");
  while (searchResults.firstChild) {
    searchResults.removeChild(searchResults.firstChild);
  }

  // Get the user's search query from the form input field
  const searchValue = document.getElementById("query").value;
  console.log(`Search query: ${searchValue}`);

  try {
    const itemsPerPage = 10;
    const startIndex = (page - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;

    // // Replace the URL with your Java server's URL
    // const serverUrl = 'https://your-java-server-url.com';

    // // Use Axios to make a GET request to your Java server's API endpoint
    // const response = await axios.get(`${serverUrl}/api/search`, {
    //   params: {
    //     q: searchValue,
    //   },
    // });

    // const data = response.data;

    let data;
    if (searchValue == "2") {
      data = dummyData2;
    } else if (searchValue == "3") {
      data = dummyData3;
    } else if (searchValue == "long") {
      data = dummyDataLong;
    }
      else {
      data = dummyData1;
    }

    const results = data.results.slice(startIndex, endIndex);
    const totalPages = Math.ceil(data.results.length / itemsPerPage);

    // Create a new unordered list to hold the search results
    const resultList = document.createElement("ul");

    // Loop through each search result and create a new list item for it
    results.forEach((result) => {
      // Create a new list item element
      const listItem = document.createElement("li");
      listItem.classList.add("search-result");

      // Create a new heading element for the search result title, with a link to the full article
      const title = document.createElement("h3");
      const link = document.createElement("a");
      link.href = `/result/${result.id}`;
      link.textContent = result.title;
      title.appendChild(link);

      // Create a new paragraph element for the search result description
      const description = document.createElement("p");
      description.textContent = result.description;

      // Add the title and description to the list item
      listItem.appendChild(title);
      listItem.appendChild(description);

      // Add the list item to the unordered list of search results
      resultList.appendChild(listItem);
    });

    // Add the unordered list of search results to the page
    searchResults.appendChild(resultList);

    // Create and add page navigation buttons
    const nav = document.createElement("nav");
    for (let i = 1; i <= totalPages; i++) {
      const button = document.createElement("button");
      button.textContent = i;
      button.classList.add("page-button");

      if (i === page) {
        button.disabled = true;
      }

      button.addEventListener("click", (event) => search(event, i));
      nav.appendChild(button);
    }
    searchResults.appendChild(nav);
  } catch (error) {
    // If there's an error fetching search results, display an error message on the page
    console.error("Error fetching search results:", error);
    searchResults.innerHTML =
      "<p>There was an error fetching search results. Please try again later.</p>";
  }
}


const dummyDataLong = {
  results: [
    // You can use a for loop to generate 100 dummy items
    ...Array(200).keys()].map((i) => {
      return {
        id: i + 1,
        title: `Dummy Title ${i + 1}`,
        description: `This is a dummy description for the result item ${i + 1}.`,
      };
    })
};





const dummyData1 = {results: [
    {
        id: 1,
        title: "Search Result 1",
        description: "Here's a short description of the first search result. This is a placeholder text to demonstrate what a search result might look like.",
    },
    {
        id: 2,
        title: "Search Result 2",
        description: "Here's a short description of the second search result. This is a placeholder text to demonstrate what a search result might look like.",
    },
    {
        id: 3,
        title: "Search Result 3",
        description: "Here's a short description of the third search result. This is a placeholder text to demonstrate what a search result might look like.",
    },
    {
        id: 4,
        title: "Search Result 4",
        description: "Here's a short description of the fourth search result. This is a placeholder text to demonstrate what a search result might look like.",
    },
    {
        id: 5,
        title: "Search Result 5",
        description: "Here's a short description of the fifth search result. This is a placeholder text to demonstrate what a search result might look like.",
    },
]};


const dummyData2 = {
    results: [
      {
        id: 1,
        title: "How to Make the Perfect Cup of Coffee",
        description: "Learn how to make a delicious and satisfying cup of coffee at home, including tips for choosing the best beans and brewing methods.",
      },
      {
        id: 2,
        title: "The Benefits of Yoga for Mind and Body",
        description: "Discover the many ways that practicing yoga can improve your physical and mental health, including increased flexibility, strength, and relaxation.",
      },
      {
        id: 3,
        title: "10 Easy and Healthy Dinner Recipes",
        description: "Get inspired for your next meal with these simple and nutritious dinner ideas, perfect for busy weeknights or lazy weekends.",
      },
      {
        id: 4,
        title: "The Top 5 Tourist Attractions in Paris",
        description: "Explore the most popular sights and experiences in the City of Light, from the iconic Eiffel Tower to the charming streets of Montmartre.",
      },
      {
        id: 5,
        title: "How to Build a Successful Online Business",
        description: "Learn the essential steps for launching and growing a profitable online business, including strategies for marketing, sales, and customer service.",
      },
    ],
  };

  const dummyData3 = {
    results: [
      {
        id: 1,
        title: "The Best Books of 2023: A Reading List",
        description: "Discover the most anticipated books of the year, from bestselling authors to up-and-coming debut novelists. Get ready to add these titles to your reading list!",
      },
      {
        id: 2,
        title: "10 Must-Visit Destinations for Adventure Travel",
        description: "Are you a thrill-seeker looking for your next adventure? Check out these top travel destinations, perfect for hiking, biking, kayaking, and more.",
      },
      {
        id: 3,
        title: "How to Create a Capsule Wardrobe for Any Occasion",
        description: "Simplify your closet and elevate your style with a capsule wardrobe. Learn how to choose versatile pieces that can be mixed and matched for any occasion.",
      },
      {
        id: 4,
        title: "5 Tips for Better Time Management and Productivity",
        description: "Are you struggling to balance your work and personal life? Check out these time management tips and tricks for boosting your productivity and achieving your goals.",
      },
      {
        id: 5,
        title: "The Benefits of Meditation for Stress and Anxiety",
        description: "Discover the science-backed benefits of meditation for reducing stress and anxiety, improving focus and concentration, and promoting overall well-being.",
      },
    ],
  };
  
//   // Update your form event listener to call the search function with the initial page number
// const form = document.getElementById("search-form");
// form.addEventListener("submit", (event) => search(event, 1));